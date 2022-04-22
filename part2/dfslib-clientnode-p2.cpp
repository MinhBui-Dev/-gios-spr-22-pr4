#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using dfs_service::file;
using dfs_service::fileData;
using dfs_service::Empty;
using dfs_service::fileDetail;
using dfs_service::lockRequest;
using dfs_service::fileAttr;
using dfs_service::fileListAttr;
using dfs_service::callbackfile;

using std::chrono::system_clock;
using std::chrono::milliseconds;

extern dfs_log_level_e DFS_LOG_LEVEL;

//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = callbackfile;
using FileListResponseType = fileListAttr;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}


grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
    ClientContext context;
    context.set_deadline(system_clock::now()+milliseconds(deadline_timeout));

    std::string filePath = WrapPath(filename);
    struct stat fileSt;

    // Check if file exists on client
    if (stat(filePath.c_str(),&fileSt) != 0)
    {
        dfs_log(LL_SYSINFO) << "File: " << filename << " cannot be found/open.";
        return StatusCode::NOT_FOUND;
    }
    
    StatusCode lockStatus = this->RequestWriteAccess(filename);

    if (lockStatus != StatusCode::OK)
    {
        dfs_log(LL_SYSINFO) << "Failed to request write lock.";
        return StatusCode::RESOURCE_EXHAUSTED;
    }


    // Get size and open file
    int fileSize = fileSt.st_size;
    std::ifstream myFile(filePath);

    // Create client writer and server response
    fileDetail serverResp;
    fileData data;
    std::unique_ptr<::grpc::ClientWriter<::dfs_service::fileData>> writer (service_stub->storeFile(&context, &serverResp));
    
    // Create fileData message and send filename
    long int timeMod = static_cast<long int>(fileSt.st_mtime);
    uint32_t client_crc = dfs_file_checksum(filePath, &this->crc_table);

    // set fileName, crc, timeMod
    data.mutable_file()->set_filename(filename);
    data.mutable_file()->set_timemodified(timeMod);
    data.mutable_file()->set_crc(client_crc);

    dfs_log(LL_SYSINFO) << "Request at: " << data.file().filename() << " to store";
    writer->Write(data);

    int byteSent = 0;
    int byteToRead = 0;
    // loop to read/send file content
    while (byteSent < fileSize)
    {
        
        byteToRead = MIN(fileSize - byteSent,CHUNK_SIZE);
        char buffer[CHUNK_SIZE];
        myFile.read(buffer,byteToRead);
        data.set_data(buffer,byteToRead);
        writer -> Write(data);
        byteSent += byteToRead;
    }

    // Close writer and file
    myFile.close();
    writer->WritesDone();

    Status status = writer -> Finish();
    
    if (!status.ok())
    {
        if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_SYSINFO) << "Request at: " << filename << " exceed deadline.";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        if (status.error_code() == StatusCode::ALREADY_EXISTS)
        {
            dfs_log(LL_SYSINFO) << "Store file: " << filename << " already exist.";
            return status.error_code();
        }
        
        
        return StatusCode::CANCELLED;
    }
    
    dfs_log(LL_SYSINFO) << "File: " << filename << " stored to server!";
    return status.error_code();

}


grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    ClientContext context;
    context.set_deadline(system_clock::now()+milliseconds(deadline_timeout));

    std::string clientId = ClientId();

    lockRequest request;
    request.set_clientid(client_id);
    request.mutable_filedetail()->set_filename(filename);
    Empty resp;

    Status status = service_stub->requestLock(&context,request,&resp);

    if (!status.ok())
    {
        if (status.error_code() == StatusCode::NOT_FOUND)
        {
            dfs_log(LL_SYSINFO) << "File: " << filename << " cannot found";
            return StatusCode::NOT_FOUND;
        }
        else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_SYSINFO) << "Request at: " << filename << " exceed deadline";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else
        return StatusCode::CANCELLED;
    }
    
    return status.error_code();


}

grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
    

    StatusCode lockStatus = this->RequestWriteAccess(filename);

    if (lockStatus != StatusCode::OK)
    {
        dfs_log(LL_SYSINFO) << "Failed to request write lock.";
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    ClientContext context;
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    file requestFile;
    requestFile.set_filename(filename);

    // Create fileData and client reader
    std::unique_ptr<::grpc::ClientReader<::dfs_service::fileData>> reader(service_stub->fetchFile(&context, requestFile));

    fileData data;

    reader -> Read(&data);
    fileDetail serverFile = data.file();
    long int serverTimeMod = serverFile.timemodified();
    uint32_t server_crc = data.file().crc();

    std::string filePath = WrapPath(filename);
    struct stat fileSt;

    // Check if file already exists on server
    if(stat(filePath.c_str(),&fileSt) == 0)
    {   
        // Check if timeMod is the same 
        long int clientTimeMod = static_cast<long int>(fileSt.st_mtime);
        if (serverTimeMod == clientTimeMod)
        {

            std::string msg = "Files: "+filename+" from client and server are same version .";
            dfs_log(LL_SYSINFO) << msg;
            return StatusCode::ALREADY_EXISTS;
        }
        else
        {   
            // validate checksum to see if the content is the same
            uint32_t client_crc = dfs_file_checksum(filePath, &this->crc_table);
            if (server_crc == client_crc)
            {
                // if content is the same and client time is newer
                if (clientTimeMod < serverTimeMod)
                {
                    // Update modTime on client file
                    struct utimbuf ub;
                    ub.modtime = serverTimeMod;
                    ub.actime = fileSt.st_atime;
                    utime(filePath.c_str(),&ub);
                }

                std::string msg = "Client File: "+filename+" is up to date on client.";
                dfs_log(LL_SYSINFO) << msg;
                return StatusCode::ALREADY_EXISTS;
            }
        }      
    }
    // store file to client
    std::ofstream myFile;

    while (reader -> Read(&data))
    {
        // Only open file if reader receives data
        if(!myFile.is_open())
        {
            myFile.open(filePath,std::ios::trunc);
        }

        const std::string& str = data.data();
        myFile << str;
    }

    // Check status received from server
    Status status =  reader -> Finish();
    myFile.close();
    dfs_log(LL_DEBUG) << "Check error code: " << status.error_code();
    
    if (!status.ok())
    {   
        // Check if file not found
        if (status.error_code() == StatusCode::NOT_FOUND)
        {
            dfs_log(LL_SYSINFO) << "File: " << filePath << " cannot found";
            return StatusCode::NOT_FOUND;
        }
        else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_SYSINFO) << "Request at: " << filePath << " exceed deadline";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else
        {
            dfs_log(LL_SYSINFO) << "Internal Error";
            return StatusCode::CANCELLED;
        }
        
    }
    
    

    dfs_log(LL_DEBUG) << "Finished fetching file: " << filename;
    return status.error_code();
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //

    // Create client context and set deadline
    

    // Request file modification lock
    StatusCode lockStatus = this->RequestWriteAccess(filename);
    if (lockStatus != StatusCode::OK)
    {
        dfs_log(LL_SYSINFO) << "Failed to request write lock.";
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    ClientContext context;
    context.set_deadline(system_clock::now()+milliseconds(deadline_timeout));

    // Create file request and set file name
    file file;
    file.set_filename(filename);

    // Create server response
    fileDetail serverResp;

    // Call rpc deleteFile
    Status status = service_stub->deleteFile(&context, file, &serverResp);

    // Check return status
    if (!status.ok())
    {   
        // Check if file not found
        if (status.error_code() == StatusCode::NOT_FOUND)
        {
            dfs_log(LL_ERROR) << "File: " << filename << " cannot found";
            return StatusCode::NOT_FOUND;
        }
        else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_ERROR) << "Request at: " << filename << " exceed deadline";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else
        return StatusCode::CANCELLED;
    }
    
    return status.error_code();

}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
    // Create client context and set deadline
    ClientContext context;
    context.set_deadline(system_clock::now()+milliseconds(deadline_timeout));

    // Create server response & client request
    fileListAttr serverResp;
    callbackfile request;  

    // Call rpc getStatus
    Status status = service_stub->listFile(&context,request,&serverResp);

    // Check return status
    if (!status.ok())
    {   
        // Check if file not found
        if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
        {
            return StatusCode::DEADLINE_EXCEEDED;
        }
        
        return StatusCode::CANCELLED;
    }
    
    // Mapping file name and mtime to file_map
    for (const fileAttr file : serverResp.file())
    {   
        file_map->insert(std::pair<std::string,int>(file.filename(),file.timemodified()));
    }
    
    return status.error_code();
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
    // Create client context and set deadline
    ClientContext context;
    context.set_deadline(system_clock::now()+milliseconds(deadline_timeout));

    // Create file request and set file name
    file file;
    file.set_filename(filename);

    // Create server response
    fileAttr serverResp;

    // Call rpc getStatus
    Status status = service_stub->getStatus(&context, file, &serverResp);

    // Check return status
    if (!status.ok())
    {   
        // Check if file not found
        if (status.error_code() == StatusCode::NOT_FOUND)
        {
            dfs_log(LL_ERROR) << "File: " << filename << " cannot found";
            return StatusCode::NOT_FOUND;
        }
        else if (status.error_code() == StatusCode::DEADLINE_EXCEEDED)
        {
            dfs_log(LL_ERROR) << "Request at: " << filename << " exceed deadline";
            return StatusCode::DEADLINE_EXCEEDED;
        }
        else
        return StatusCode::CANCELLED;
    }
    
    // return serverResp to arg file_status
    file_status = &serverResp;

    return status.error_code();
}   

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback) {

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //

    dirMutex.lock();

    callback();
    dfs_log(LL_SYSINFO) << "Finished Inotify Watcher Callback and Unlock Mutex";

    dirMutex.unlock();
}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList() {

    void* tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok)) {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok) {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok()) {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //
                dirMutex.lock();
                for (const fileAttr& serverFile:call_data->reply.file())
                {
                    std::string fileName = serverFile.filename();
                    std::string filePath = WrapPath(fileName);
                    struct stat fileSt;
                    if (stat(filePath.c_str(),&fileSt) != 0)
                    {
                        dfs_log(LL_SYSINFO) << "Remote file: " << fileName << " cannot found";
                        dfs_log(LL_SYSINFO) << "Fetch file: " << fileName << " from server now";
                        this->Fetch(fileName);
                    }
                    else
                    {
                       long int serverTimeMod = serverFile.timemodified();
                       long int clientTimeMod = static_cast<long int>(fileSt.st_mtime);
                       uint32_t server_crc = serverFile.crc();
                       uint32_t client_crc = dfs_file_checksum(filePath, &this->crc_table);
                       // client file is more recent
                       if (serverTimeMod < clientTimeMod)
                       {    
                            dfs_log(LL_SYSINFO) << "Client file: " << fileName << " is newer. Store to server now.";
                            this->Store(fileName);
                       }
                       else if (clientTimeMod < serverTimeMod && server_crc == client_crc)
                       {
                            dfs_log(LL_SYSINFO) << "Client file: " << fileName << " is updated with server's time";
                            struct utimbuf ub;
                            ub.modtime = serverTimeMod;
                            ub.actime = fileSt.st_atime;
                            utime(filePath.c_str(),&ub);
                       }
                       else if (clientTimeMod < serverTimeMod && server_crc != client_crc)
                       {
                           dfs_log(LL_SYSINFO) << "Remote file: " << fileName << " is more updated. Fetch now.";
                           this->Fetch(fileName);
                       }
                        
                    }
                    
                }

                dirMutex.unlock();

            } else {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here

        }


        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();

    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList() {
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//


