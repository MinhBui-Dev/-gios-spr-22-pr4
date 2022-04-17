#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <csignal>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <google/protobuf/util/time_util.h>

#include "dfslib-shared-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "dfslib-clientnode-p1.h"

using grpc::Status;
using grpc::Channel;
using grpc::StatusCode;
using grpc::ClientWriter;
using grpc::ClientReader;
using grpc::ClientContext;

using dfs_service::fileData;
using dfs_service::file;
using dfs_service::fileDetail;
using dfs_service::fileList;
using dfs_service::fileAttr;
using dfs_service::Empty;

using std::chrono::system_clock;
using std::chrono::milliseconds;
//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//


DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode() {}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
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
        dfs_log(LL_ERROR) << "File Stat: " << filePath << " error";
        return StatusCode::NOT_FOUND;
    }
    
    // Get size and open file
    int fileSize = fileSt.st_size;
    std::ifstream myfile(filePath);

    // Create client writer and server response
    fileDetail serverResp;
    std::unique_ptr<::grpc::ClientWriter<::dfs_service::fileData>> writer (service_stub->storeFile(&context, &serverResp));
    
    // Create fileData message and send filename
    fileData data;
    data.set_filename(filename);
    writer->Write(data);

    int byteSent = 0;
    int byteToRead = 0;
    // loop to read/send file content
    while (byteSent < fileSize)
    {
        char buffer[CHUNK_SIZE];
        byteToRead = MIN(fileSize - byteSent,CHUNK_SIZE);
        myfile.read(buffer,byteToRead);
        data.set_data(buffer,byteToRead);
        writer -> Write(data);
        byteSent += byteToRead;
    }

    // Close writer and file
    myfile.close();
    writer->WritesDone();
    Status status = writer -> Finish();
    
    if (!status.ok())
    {
        return StatusCode::CANCELLED;
    }
    
    return status.error_code();

}


StatusCode DFSClientNodeP1::Fetch(const std::string &filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    // Set deadline_timeout for client
    ClientContext context;
    context.set_deadline(system_clock::now() + milliseconds(deadline_timeout));

    // Create file name request
    file file;
    file.set_filename(filename);

    // Create fileData and client reader
    fileData data;
    std::unique_ptr<::grpc::ClientReader<::dfs_service::fileData>> reader(service_stub->fetchFile(&context, file));

    // Create file system for write on client
    std::string filePath = WrapPath(filename);
    std::ofstream myfile;

    // Reader read from server
    while (reader -> Read(&data))
    {
        // Open file on first read
        if(!myfile.is_open())
        myfile.open(filePath,std::ios::trunc);

        const std::string &str = data.data();
        myfile << str;
    }

    // Close file if open
    if (myfile.is_open())
    {
         myfile.close();
    }
    
   
    // Check status received from server
    Status status =  reader -> Finish();

  
    if (!status.ok())
    {   
        // Check if file not found
        if (status.error_code() == StatusCode::NOT_FOUND)
        {
            return StatusCode::NOT_FOUND;
        }
        
        return StatusCode::CANCELLED;
    }
    
    return status.error_code();





}

StatusCode DFSClientNodeP1::Delete(const std::string& filename) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
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
    fileDetail serverResp;

    // Call rpc deleteFile
    Status status = service_stub->deleteFile(&context, file, &serverResp);

    // Check return status
    if (!status.ok())
    {   
        // Check if file not found
        if (status.error_code() == StatusCode::NOT_FOUND)
        {
            return StatusCode::NOT_FOUND;
        }
        
        return StatusCode::CANCELLED;
    }
    
    return status.error_code();

}


StatusCode DFSClientNodeP1::Stat(const std::string &filename, void* file_status) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
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
            return StatusCode::NOT_FOUND;
        }
        
        return StatusCode::CANCELLED;
    }
    
    // return serverResp to arg file_status
    file_status = &serverResp;

    return status.error_code();
}

StatusCode DFSClientNodeP1::List(std::map<std::string,int>* file_map, bool display) {

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
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
    fileList serverResp;
    Empty request;  

    // Call rpc getStatus
    Status status = service_stub->listFile(&context,request,&serverResp);

    // Check return status
    if (!status.ok())
    {   
        // Check if file not found
        if (status.error_code() == StatusCode::NOT_FOUND)
        {
            return StatusCode::NOT_FOUND;
        }
        
        return StatusCode::CANCELLED;
    }
    
    // Mapping file name and mtime to file_map
    for (const fileDetail file : serverResp.file())
    {   
        // Convert google timestamp to int type
        int timeSec = google::protobuf::util::TimeUtil::TimestampToSeconds(file.timemodified());
        file_map->insert(std::pair<std::string,int>(file.filename(),timeSec));
    }
    
    return status.error_code();
}
//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//

