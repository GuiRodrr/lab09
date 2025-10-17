#pragma once

#include <grpcpp/grpcpp.h>
#include "file_processor.grpc.pb.h"

using grpc::ServerContext;
using grpc::Status;
using file_processor::FileProcessorService;
using file_processor::FileRequest;
using file_processor::FileResponse;
using file_processor::FileChunk;

class FileProcessorServiceImpl final : public FileProcessorService::Service {
public:
    Status CompressPDF(ServerContext* context, const FileRequest* request, FileResponse* response) override;
    Status ConvertToTXT(ServerContext* context, const FileRequest* request, FileResponse* response) override;
    Status ConvertImageFormat(ServerContext* context, const FileRequest* request, FileResponse* response) override;
    Status ResizeImage(ServerContext* context, const FileRequest* request, FileResponse* response) override;

private:
    std::string SaveInputFile(const std::string& file_name, const google::protobuf::RepeatedPtrField<FileChunk>& chunks);
    std::string ReadOutputFileChunks(const std::string& file_path, FileResponse* response);
    void Log(const std::string& service, const std::string& file, const std::string& message, bool success);
};
