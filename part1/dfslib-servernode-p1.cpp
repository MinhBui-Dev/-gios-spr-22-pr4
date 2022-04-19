#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <iostream>
#include <fstream>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <getopt.h>
#include <grpcpp/grpcpp.h>

#include "src/dfs-utils.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "dfslib-shared-p1.h"


using grpc::Status;
using grpc::Server;
using grpc::StatusCode;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::ServerContext;
using grpc::ServerBuilder;

using dfs_service::DFSService;
using dfs_service::fileData;
using dfs_service::file;
using dfs_service::fileDetail;
using dfs_service::fileList;
using dfs_service::fileAttr;
using dfs_service::Empty;

//
// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service {

private:

    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath) {
        return this->mount_path + filepath;
    }


public:

    DFSServiceImpl(const std::string &mount_path): mount_path(mount_path) {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //

    Status storeFile(
        ServerContext* context, 
        ServerReader< fileData>* reader, 
        fileDetail* response) override {
        
        fileData data;
        
        reader-> Read(&data);
        const std::string fileName = data.filename();
        const std::string filePath = WrapPath(fileName);

        // Create output stream handler file
        std::ofstream myFile;

        while (reader -> Read(&data))
        {
            // Only open file if reader receives data
            if(!myFile.is_open())
            myFile.open(filePath,std::ios::trunc);
            
            // Rechecking timeout from client each time reading message
            if (context->IsCancelled())
            {
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }

            const std::string str = data.data();
            myFile << str;
        }

        struct stat fileSt;
        if (stat(filePath.c_str(),&fileSt) != 0)
        {
            dfs_log(LL_ERROR) << "File: " << filePath << " cannot be found/open.";
            return Status(StatusCode::NOT_FOUND, "Struct Stat cannot open file.");
        }

        long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);

        response->set_filename(fileName);
        response->set_timemodified(timeMod);
         
        return Status::OK;
        
    }

    Status fetchFile(
        ServerContext* context, 
        const file* request, 
        ServerWriter<fileData>* writer) override{

        const std::string fileName = request->filename();
        const std::string filePath = WrapPath(fileName);

        struct stat fileSt;
        if (stat(filePath.c_str(),&fileSt) != 0)
        {
            dfs_log(LL_ERROR) << "File Stat: " << filePath << " cannot be open/found!";
            return Status(StatusCode::NOT_FOUND, "Struct Stat cannot open file.");
        }

        int fileSize = fileSt.st_size;
        std::ifstream myFile;

        myFile.open(filePath, std::ios::binary);

        fileData data;

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

        dfs_log(LL_DEBUG) << "Finished fetching file: " << request->filename();
        return Status::OK;
    }

    Status deleteFile(
        ServerContext* context,
        const file* request,
        fileDetail* response) override {

        const std::string fileName = request->filename();
        const std::string filePath = WrapPath(fileName);

        struct stat fileSt;

        if (stat(filePath.c_str(), &fileSt) != 0)
        {
            dfs_log(LL_ERROR) << "File Stat: " << filePath << " error";
            return Status(StatusCode::NOT_FOUND, "Struct Stat cannot open file.");
        }
        
        if (context->IsCancelled())
        {
            dfs_log(LL_ERROR) << "Request at: " << filePath << " exceed deadline";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        if (remove(filePath.c_str()) != 0)
        {
            dfs_log(LL_ERROR) << "File: " << filePath << " failed to remove";
            return Status(StatusCode::INTERNAL, "Error on removing file");
        }
        
        long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);
        response->set_filename(fileName);
        response->set_timemodified(timeMod);
        
        return Status::OK;
        
    }

    Status listFile(
        ServerContext* context,
        const Empty* request,
        fileList* response) override {

        
        // create directory handler
        DIR *dir;
        struct dirent *ent;
        

        if ((dir = opendir(mount_path.c_str())) == NULL)
        {
            dfs_log(LL_ERROR) << "Failed to open directory: " << mount_path;
            return Status(StatusCode::INTERNAL, "Error on opening directory");
        }

        while ((ent = readdir(dir))!= NULL)
        {
            if (context->IsCancelled())
            {
                dfs_log(LL_SYSINFO) << "Request exceeded deadline";
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }

            struct stat fileSt;
            const std::string fileName = ent ->d_name;
            const std::string filePath = WrapPath(fileName);
            stat(filePath.c_str(), &fileSt);
            if (!S_ISREG(fileSt.st_mode))
            {
                dfs_log(LL_SYSINFO) << "Not regular file " << fileName;
                continue;
            }
            fileDetail* file = response->add_file();
            long int timeMod = static_cast<long int>(fileSt.st_mtim.tv_sec);
            file->set_filename(fileName);
            file->set_timemodified(timeMod);            
        }
        
        closedir(dir);
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

        response->set_filename(fileName);
        response->set_timecreated(timeCreate);
        response->set_timemodified(timeMod);

        return Status::OK;

    }

};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these three
// methods as-is.
//
/**
 * The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
        const std::string &mount_path,
        std::function<void()> callback) :
    server_address(server_address), mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept {
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start() {
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//

