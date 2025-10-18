#include <grpcpp/grpcpp.h>
#include <grpcpp/server_context.h>
#include <grpcpp/server_builder.h>
#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <vector>
#include <cstdlib>
#include <filesystem>

// Inclua os headers gerados pelo protobuf
#include "proto/file_processor.grpc.pb.h"
#include "proto/file_processor.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

// Use o namespace gerado pelo protobuf
using namespace file_processor;

// Funções auxiliares para logging
void LogSuccess(const std::string& method, const std::string& filename, const std::string& message) {
    std::cout << "[SUCCESS][" << method << "] " << filename << ": " << message << std::endl;
}

void LogError(const std::string& method, const std::string& filename, const std::string& message) {
    std::cerr << "[ERROR][" << method << "] " << filename << ": " << message << std::endl;
}

class FileProcessorServiceImpl final : public FileProcessor::Service {
public:
    Status CompressPDF(ServerContext* context, const FileRequest* request, FileResponse* response) override {
        std::string input_file_path = "/tmp/input_" + request->file_name();
        std::string output_file_path = "/tmp/output_" + request->file_name();

        // Salvar arquivo temporário
        std::ofstream input_file(input_file_path, std::ios::binary);
        if (!input_file) {
            LogError("CompressPDF", request->file_name(), "Falha ao criar arquivo temporário de entrada.");
            response->set_success(false);
            response->set_status_message("Erro no servidor ao criar arquivo temporário.");
            return Status(grpc::StatusCode::INTERNAL, "Erro ao criar arquivo temporário");
        }
        input_file.write(request->file_content().data(), request->file_content().size());
        input_file.close();

        // Comando Ghostscript para compressão
        std::string command = "gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/ebook -dNOPAUSE -dQUIET -dBATCH -sOutputFile=" + output_file_path + " " + input_file_path;
        
        int gs_result = std::system(command.c_str());
        
        if (gs_result == 0) {
            // Ler arquivo comprimido
            std::ifstream output_file(output_file_path, std::ios::binary);
            if (output_file) {
                std::string compressed_content((std::istreambuf_iterator<char>(output_file)), 
                                             std::istreambuf_iterator<char>());
                output_file.close();
                
                response->set_success(true);
                response->set_file_name("compressed_" + request->file_name());
                response->set_file_content(compressed_content);
                
                LogSuccess("CompressPDF", request->file_name(), "Compressão PDF bem-sucedida.");
                
                // Limpar arquivos temporários
                std::remove(input_file_path.c_str());
                std::remove(output_file_path.c_str());
                
                return Status::OK;
            } else {
                LogError("CompressPDF", request->file_name(), "Falha ao abrir arquivo comprimido para envio.");
                response->set_success(false);
                response->set_status_message("Erro no servidor ao abrir arquivo comprimido.");
                return Status(grpc::StatusCode::INTERNAL, "Erro ao abrir arquivo comprimido");
            }
        } else {
            LogError("CompressPDF", request->file_name(), "Falha na compressão PDF. Código de retorno: " + std::to_string(gs_result));
            response->set_success(false);
            response->set_status_message("Falha ao comprimir PDF.");
            return Status(grpc::StatusCode::INTERNAL, "Falha na compressão PDF");
        }
    }

    Status ConvertToTXT(ServerContext* context,
                       ServerReaderWriter<FileChunk, FileChunk>* stream) override {
        FileChunk chunk;
        std::string full_content;
        std::string filename;

        // Receber dados do cliente
        while (stream->Read(&chunk)) {
            if (filename.empty()) {
                filename = chunk.file_name();
            }
            full_content.append(chunk.chunk_data());
            if (chunk.is_last()) {
                break;
            }
        }

        if (full_content.empty()) {
            LogError("ConvertToTXT", filename, "Nenhum dado recebido");
            return Status(grpc::StatusCode::INTERNAL, "Nenhum dado recebido");
        }

        // Simular conversão para TXT
        std::string txt_content = "Texto extraído do arquivo: " + filename + "\n\n";
        txt_content += "[Conteúdo convertido para texto]\n";
        
        // Enviar resposta em chunks
        size_t chunk_size = 4096;
        for (size_t i = 0; i < txt_content.size(); i += chunk_size) {
            FileChunk response_chunk;
            response_chunk.set_file_name(filename + ".txt");
            response_chunk.set_chunk_data(txt_content.substr(i, chunk_size));
            response_chunk.set_is_last(i + chunk_size >= txt_content.size());
            stream->Write(response_chunk);
        }

        LogSuccess("ConvertToTXT", filename, "Conversão para TXT bem-sucedida.");
        return Status::OK;
    }

    Status ConvertImageFormat(ServerContext* context,
                            ServerReaderWriter<FileChunk, FileChunk>* stream) override {
        FileChunk chunk;
        std::vector<char> image_data;
        std::string filename;

        // Receber dados do cliente
        while (stream->Read(&chunk)) {
            if (filename.empty()) {
                filename = chunk.file_name();
            }
            std::string chunk_data = chunk.chunk_data();
            image_data.insert(image_data.end(), chunk_data.begin(), chunk_data.end());
            if (chunk.is_last()) {
                break;
            }
        }

        if (image_data.empty()) {
            LogError("ConvertImageFormat", filename, "Nenhum dado de imagem recebido");
            return Status(grpc::StatusCode::INTERNAL, "Nenhum dado de imagem recebido");
        }

        // Simular conversão de formato
        std::string converted_data(image_data.begin(), image_data.end());
        std::string new_filename = "converted_" + filename + ".png";

        // Enviar resposta
        FileChunk response_chunk;
        response_chunk.set_file_name(new_filename);
        response_chunk.set_chunk_data(converted_data);
        response_chunk.set_is_last(true);
        stream->Write(response_chunk);

        LogSuccess("ConvertImageFormat", filename, "Conversão de formato bem-sucedida.");
        return Status::OK;
    }

    Status ResizeImage(ServerContext* context,
                      ServerReaderWriter<FileChunk, FileChunk>* stream) override {
        FileChunk chunk;
        std::vector<char> image_data;
        std::string filename;

        // Receber dados do cliente
        while (stream->Read(&chunk)) {
            if (filename.empty()) {
                filename = chunk.file_name();
            }
            std::string chunk_data = chunk.chunk_data();
            image_data.insert(image_data.end(), chunk_data.begin(), chunk_data.end());
            if (chunk.is_last()) {
                break;
            }
        }

        if (image_data.empty()) {
            LogError("ResizeImage", filename, "Nenhum dado de imagem recebido");
            return Status(grpc::StatusCode::INTERNAL, "Nenhum dado de imagem recebido");
        }

        // Simular redimensionamento
        std::string resized_data(image_data.begin(), image_data.end());
        std::string new_filename = "resized_" + filename;

        // Enviar resposta
        FileChunk response_chunk;
        response_chunk.set_file_name(new_filename);
        response_chunk.set_chunk_data(resized_data);
        response_chunk.set_is_last(true);
        stream->Write(response_chunk);

        LogSuccess("ResizeImage", filename, "Redimensionamento de imagem bem-sucedido.");
        return Status::OK;
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    FileProcessorServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Servidor ouvindo em " << server_address << std::endl;
    server->Wait();
}

int main(int argc, char** argv) {
    RunServer();
    return 0;
}