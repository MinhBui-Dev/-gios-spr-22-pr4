#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <getopt.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include <dirent.h>
#include <utime.h>

#include "src/dfslibx-service-runner.h"
#include "dfslib-shared-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "dfslib-servernode-p2.h"

using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;

using dfs_service::file;
using dfs_service::fileData;
using dfs_service::Empty;
using dfs_service::fileDetail;
using dfs_service::lockRequest;
using dfs_service::fileAttr;
using dfs_service::fileListAttr;
using dfs_service::callbackfile;
//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//

using FileRequestType = callbackfile;
using FileListResponseType = fileListAttr;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final :
    public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
        public DFSCallDataManager<FileRequestType , FileListResponseType> {

private:

    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The mount path for the server **/
    std::string mount_path;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

    // Map object to store <filename and clientId
    std::map<std::string, std::string> fileMap;

    // Mutex controlling access to fileNameToClientId map
    std::shared_timed_mutex fileMutex;
    
    // Mutex to control directory access
    std::shared_timed_mutex dirMutex;

    // helper function to delete fileName and ClientId from fileMap
    void releaseFileLock(const std::string fileName){
         std::lock_guard<std::shared_timed_mutex> lock(fileMutex);
         fileMap.erase(fileName);
    }

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

public:

    DFSServiceImpl(const std::string& mount_path, const std::string& server_address, int num_async_threads):
        mount_path(mount_path), crc_table(CRC::CRC_32()) {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]{ this->ProcessQueuedRequests(); });

    }

    ~DFSServiceImpl() {
        this->runner.Shutdown();
    }

    void Run() {
        this->runner.Run();
    }

    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext* context,
                         FileRequestType* request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType>* response,
                         grpc::ServerCompletionQueue* cq,
                         void* tag) {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);

    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext* context, FileRequestType* request, FileListResponseType* response) {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        this->CallbackList(context,request,response);

    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests() {
        while(true) {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //


            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);


                for(QueueRequest<FileRequestType, FileListResponseType>& queue_request : this->queued_tags) {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                        queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                    this->queued_tags.begin(),
                    this->queued_tags.end(),
                    [](QueueRequest<FileRequestType, FileListResponseType>& queue_request) { return queue_request.finished; }
                ), this->queued_tags.end());

            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods. 
    //
    Status requestLock(
        ServerContext* context,
        const lockRequest* request,
        Empty* response) override {

        std::string clientID = request->clientid();
        fileDetail file = request->filedetail();
        std::string fileName = file.filename();

        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        std::lock_guard<std::shared_timed_mutex> lock(fileMutex);
        auto index = fileMap.find(fileName);
        std::string fileClientId;

        // if fileName in filemap and clientID is different Client Value
        if ((index != fileMap.end()) && ((fileClientId = index->second).compare(clientID) != 0 ))
        {
            std::string msg = "File: "+fileName+" is locked by another client.";
            dfs_log(LL_SYSINFO) << msg;
            return Status(StatusCode::RESOURCE_EXHAUSTED, msg);
        }
        // if found fileName and clientId is the same
        else if ((index != fileMap.end()) && ((fileClientId = index->second).compare(clientID) == 0 ))
        {
            std::string msg = "File: "+fileName+" is locked by same client.";
            dfs_log(LL_SYSINFO) << msg;
            return Status(StatusCode::OK, msg);
        }
        else
        {
            // Insert fileName and ClientID to fileMap
            fileMap.insert(std::pair<std::string,std::string>(fileName,clientID));
            std::string msg = "File: "+fileName+" is locked by client now.";
            dfs_log(LL_SYSINFO) << msg;
            return Status(StatusCode::OK, msg);
        }
    }
    
    Status storeFile(
        ServerContext* context,
        ServerReader<fileData>* reader,
        fileDetail* response) override{

        fileData data;
        
        // Reader read wire and get fileName, CRC, TimeMod
        reader-> Read(&data);
        fileDetail file = data.file();
        std::string fileName = file.filename();
        std::string filePath = WrapPath(fileName);
        long int clientTimeMod = file.timemodified();
        uint32_t client_crc = file.crc();

        struct stat fileSt;

        dirMutex.lock_shared();

        // Check if file already exists on server
        if(stat(filePath.c_str(),&fileSt) == 0)
        {   
            // Check if timeMod is the same 
            long int serverTimeMod = static_cast<long int>(fileSt.st_mtime);
            if (serverTimeMod == clientTimeMod)
            {
                dirMutex.unlock_shared();
                releaseFileLock(fileName);
                std::string msg = "Files: "+fileName+" from client and server are same version .";
                dfs_log(LL_SYSINFO) << msg;
                return Status(StatusCode::ALREADY_EXISTS, msg);
            }
            else
            {   
                // validate checksum to see if the content is the same
                uint32_t server_crc = dfs_file_checksum(filePath, &this->crc_table);
                if (server_crc == client_crc)
                {
                    // if content is the same and client time is newer
                    if (serverTimeMod < clientTimeMod)
                    {
                        // Update modTime on server file
                        struct utimbuf ub;
                        ub.modtime = clientTimeMod;
                        ub.actime = fileSt.st_atime;
                        utime(filePath.c_str(),&ub);
                     
                    }
                    // Unlock directory and release file lockS
                    dirMutex.unlock_shared();
                    releaseFileLock(fileName);

                    std::string msg = "File: "+fileName+" is up to date on server.";
                    dfs_log(LL_SYSINFO) << msg;
                    return Status(StatusCode::ALREADY_EXISTS, msg);
                }
            }         
        }

        // store file to server
        std::ofstream myFile;

        while (reader -> Read(&data))
        {
            // Only open file if reader receives data
            if(!myFile.is_open())
            myFile.open(filePath,std::ios::trunc);
            
            // Rechecking timeout from client each time reading message
            if (context->IsCancelled())
            {
                myFile.close();
                dirMutex.unlock_shared();
                releaseFileLock(fileName);
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }

            const std::string& str = data.data();
            myFile << str;
        }

        myFile.close();
        if (stat(filePath.c_str(),&fileSt) != 0)
        {
            dirMutex.unlock_shared();
            releaseFileLock(fileName);
            dfs_log(LL_ERROR) << "File: " << filePath << " cannot be found/open.";
            return Status(StatusCode::NOT_FOUND, "Struct Stat cannot open file.");
        }

        long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);

        dirMutex.unlock_shared();

        response->set_filename(fileName);
        response->set_timemodified(timeMod);
        releaseFileLock(fileName);

        std::string msg = "File: "+fileName+" is saved on server.";
        dfs_log(LL_SYSINFO) << msg;
        return Status(StatusCode::OK, msg);
    }
    
    Status fetchFile(
        ServerContext* context,
        const file* request,
        ServerWriter<fileData>* writer) override {
        
        const std::string fileName = request->filename();
        const std::string filePath = WrapPath(fileName);
     
        struct stat fileSt;

        // Grab shared directory mutex, eabling currently write of of dif files
        // Block if callbacklist has exclusive mutex
        // Queue for mutex if exclusive mutex call is waiting
        dirMutex.lock_shared();

        if (stat(filePath.c_str(),&fileSt) != 0)
        {
            dirMutex.unlock_shared();
        
            // Delete FileName and Client from FileMap
            releaseFileLock(fileName);
            dfs_log(LL_ERROR) << "File Stat: " << filePath << " cannot be open/found!";
            return Status(StatusCode::NOT_FOUND, "Struct Stat cannot open file.");
        }

        int fileSize = fileSt.st_size;
        
        fileData data;
        long int timeMod = static_cast<long int>(fileSt.st_mtime);
        uint32_t server_crc = dfs_file_checksum(filePath, &this->crc_table);

        // set fileName, crc, timeMod && send over filedetail data
        data.mutable_file()->set_filename(fileName);
        data.mutable_file()->set_timemodified(timeMod);
        data.mutable_file()->set_crc(server_crc);
       
        writer-> Write(data);

        std::ifstream myFile;
        myFile.open(filePath, std::ios::binary);

        int byteSent = 0;
        int byteToRead = 0;
        // dfs_log(LL_DEBUG) << "file size: " << fileSize;
        // loop to read/send file content
        while (byteSent < fileSize)
        {
            // Create buffer memory to store file content
            
            byteToRead = MIN(fileSize - byteSent,CHUNK_SIZE);
            char buffer[CHUNK_SIZE];
            // Rechecking timeout from client each time reading message
            if (context->IsCancelled())
            {
                myFile.close();

                dirMutex.unlock_shared();
        
                // Delete FileName and Client from FileMap
                releaseFileLock(fileName);

                dfs_log(LL_ERROR) << "Request at: " << filePath << " exceed deadline";
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }

            // read file content into buffer
            myFile.read(buffer,byteToRead);

            // copy content from buffer to data
            data.set_data(std::string(buffer,byteToRead));
            writer -> Write(data);

            // dfs_log(LL_DEBUG) << "Byte read to send: " << byteToRead;

            byteSent += byteToRead;
        }
        // dfs_log(LL_DEBUG) << "Byte sent total: " << byteSent;
        // Finish sending, close file and return status
        myFile.close();
        
        dirMutex.unlock_shared();
        
        // Delete FileName and Client from FileMap
        releaseFileLock(fileName);

        dfs_log(LL_DEBUG) << "Finished fetching file: " << request->filename();
        return Status::OK;

    }

    Status listFile(
        ServerContext* context,
        const callbackfile* request,
        fileListAttr* response) override {

        // create directory handler
        DIR *dir;
        struct dirent *ent;
        
        // Exclusive claim of mutex
        // Block and queue if mutex is shared_lock
        // any share_lock attempt will queue after exclusive lock()
        dirMutex.lock();

        if ((dir = opendir(mount_path.c_str())) == NULL)
        {
            dirMutex.unlock();
            dfs_log(LL_ERROR) << "Failed to open directory: " << mount_path;
            return Status(StatusCode::INTERNAL, "Error on opening directory");
        }

        while ((ent = readdir(dir))!= NULL)
        {
            
            if (context->IsCancelled())
            {
                dirMutex.unlock();
                dfs_log(LL_SYSINFO) << "Request exceeded deadline";
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            

            struct stat fileSt;
            const std::string fileName = ent ->d_name;
            const std::string filePath = WrapPath(fileName);
            stat(filePath.c_str(), &fileSt);
            if (!S_ISREG(fileSt.st_mode))
            {
                // dfs_log(LL_SYSINFO) << "Not regular file " << fileName;
                continue;
            }
            fileAttr* file = response->add_file();
            long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);
            long int timeCreate = static_cast<long int>(fileSt.st_ctim.tv_sec);
            int fileSize = fileSt.st_size;
            uint32_t server_crc = dfs_file_checksum(filePath, &this->crc_table);
            file->set_filename(fileName);
            file->set_filesize(fileSize);  
            file->set_timemodified(timeMod);   
            file->set_timecreated(timeCreate);
            file->set_crc(server_crc);
            
                
        }
        
        closedir(dir);
        dirMutex.unlock();
        return Status::OK;

    }

    Status getStatus(
        ServerContext* context,
        const file* request,
        fileAttr* response) override {
        
        const std::string fileName = request->filename();
        const std::string filePath = WrapPath(fileName);

        if (context->IsCancelled())
        {
            dfs_log(LL_SYSINFO) << "Request exceeded deadline";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }
        
        struct stat fileSt;

        if (stat(filePath.c_str(), &fileSt) != 0)
        {
            dfs_log(LL_SYSINFO) << "File: " << filePath << " cannot be found!";
            return Status(StatusCode::NOT_FOUND, "Struct Stat cannot open file.");
        }

        long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);
        long int timeCreate = static_cast<long int>(fileSt.st_ctim.tv_sec);
        int fileSize = fileSt.st_size;

        response->set_filename(fileName);
        response->set_filesize(fileSize);  
        response->set_timemodified(timeMod);   
        response->set_timecreated(timeCreate);
        
        return Status::OK;
    }

    Status CallbackList(
        ServerContext* context,
        const callbackfile* request,
        fileListAttr* response) override {
        
        // create directory handler
        DIR *dir;
        struct dirent *ent;
        
        // Exclusive claim of mutex
        // Block and queue if mutex is shared_lock
        // any share_lock attempt will queue after exclusive lock()
        dirMutex.lock();

        if ((dir = opendir(mount_path.c_str())) == NULL)
        {
            dirMutex.unlock();
            dfs_log(LL_ERROR) << "Failed to open directory: " << mount_path;
            return Status(StatusCode::INTERNAL, "Error on opening directory");
        }

        while ((ent = readdir(dir))!= NULL)
        {
            // not async-safe bahavior
            /*
            if (context->IsCancelled())
            {
                dirMutex.unlock();
                dfs_log(LL_SYSINFO) << "Request exceeded deadline";
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            */
            

            struct stat fileSt;
            const std::string fileName = ent ->d_name;
            const std::string filePath = WrapPath(fileName);
            stat(filePath.c_str(), &fileSt);
            if (!S_ISREG(fileSt.st_mode))
            {
                //dfs_log(LL_SYSINFO) << "Not regular file " << fileName;
                continue;
            }
            fileAttr* file = response->add_file();
            long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);
            long int timeCreate = static_cast<long int>(fileSt.st_ctim.tv_sec);
            int fileSize = fileSt.st_size;
            uint32_t server_crc = dfs_file_checksum(filePath, &this->crc_table);
            file->set_filename(fileName);
            file->set_filesize(fileSize);  
            file->set_timemodified(timeMod);   
            file->set_timecreated(timeCreate);
            file->set_crc(server_crc);
            
                
        }
        
        closedir(dir);
        dirMutex.unlock();
        return Status::OK;
    }

    Status deleteFile(
        ServerContext* context,
        const file* request,
        fileDetail* response) override {
        
        const std::string fileName = request->filename();
        const std::string filePath = WrapPath(fileName);

        struct stat fileSt;

        // Grab shared directory mutex, eabling currently write of of dif files
        // Block if callbacklist has exclusive mutex
        // Queue for mutex if exclusive mutex call is waiting
        

        if (stat(filePath.c_str(), &fileSt) != 0)
        {
            // Delete FileName and Client from FileMap
            releaseFileLock(fileName);

            dfs_log(LL_ERROR) << "File Stat: " << filePath << " error";
            return Status(StatusCode::NOT_FOUND, "Struct Stat cannot open file.");
        }
        
        if (context->IsCancelled())
        {
            // Delete FileName and Client from FileMap
            releaseFileLock(fileName);

            dfs_log(LL_ERROR) << "Request at: " << filePath << " exceed deadline";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }
        dirMutex.lock_shared();

        if (remove(filePath.c_str()) != 0)
        {
            dirMutex.unlock_shared();

            // Delete FileName and Client from FileMap
            releaseFileLock(fileName);

            dfs_log(LL_ERROR) << "File: " << filePath << " failed to remove";
            return Status(StatusCode::INTERNAL, "Error on removing file");
        }
        
        long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);
        response->set_filename(fileName);
        response->set_timemodified(timeMod);
        
        dirMutex.unlock_shared();
            
        // Delete FileName and Client from FileMap
        releaseFileLock(fileName);
        return Status::OK;
    }

    

};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        int num_async_threads,
        std::function<void()> callback) :
        server_address(server_address),
        mount_path(mount_path),
        num_async_threads(num_async_threads),
        grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);


    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
