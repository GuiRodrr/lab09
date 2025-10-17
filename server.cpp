#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <chrono>
#include <ctime>
#include <grpcpp/grpcpp.h>
#include "file_processor.grpc.pb.h" // Arquivo gerado pelo protobuf

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReaderWriter;
using file_processor::FileProcessorService;
using file_processor::FileRequest;
using file_processor::FileResponse;
using file_processor::FileChunk;

class FileProcessorServiceImpl final : public FileProcessorService::Service {
public:
    Status CompressPDF(ServerContext* context, const FileRequest* request, FileResponse* response) override {
        std::string input_file_path = "/tmp/input_" + request->file_name(); // Arquivo temporário
        std::string output_file_path = "/tmp/output_" + request->file_name();

        std::ofstream input_file_stream(input_file_path, std::ios::binary);
        if (!input_file_stream) {
            LogError("CompressPDF", request->file_name(), "Falha ao criar arquivo temporário de entrada.");
            response->set_success(false);
            response->set_status_message("Erro no servidor ao criar arquivo temporário.");
            return Status::INTERNAL;
        }

        // Receber stream do cliente e salvar no arquivo temporário
        grpc::ClientReaderWriter<FileChunk, FileChunk>* stream =
            grpc::ServerContext::FromServerContext(*context).ReaderWriter();

        FileChunk chunk;
        while (stream->Read(&chunk)) {
            input_file_stream.write(chunk.content().c_str(), chunk.content().size());
        }
        input_file_stream.close();

        // Executar comando gs para compressão
        std::string command = "gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 "
                              "-dPDFSETTINGS=/ebook -dNOPAUSE -dQUIET -dBATCH "
                              "-sOutputFile=" + output_file_path + " " + input_file_path;
        int gs_result = std::system(command.c_str());

        if (gs_result == 0) {
            LogSuccess("CompressPDF", request->file_name(), "Compressão PDF bem-sucedida.");
            response->set_success(true);
            response->set_file_name("compressed_" + request->file_name());

            std::ifstream output_file_stream(output_file_path, std::ios::binary);
            if (output_file_stream) {
                while (output_file_stream.peek() != EOF) {
                    FileChunk response_chunk;
                    char buffer[1024];
                    output_file_stream.read(buffer, sizeof(buffer));
                    response_chunk.set_content(buffer, output_file_stream.gcount());
                    stream->Write(response_chunk); // Enviar stream para o cliente
                }
                output_file_stream.close();
            } else {
                LogError("CompressPDF", request->file_name(), "Falha ao abrir arquivo comprimido para envio.");
                response->set_success(false);
                response->set_status_message("Erro no servidor ao abrir arquivo comprimido.");
                return Status::INTERNAL;
            }
            stream->WritesDone();
            stream->Finish();
        } else {
            LogError("CompressPDF", request->file_name(), "Falha na compressão PDF. Código de retorno: " + std::to_string(gs_result));
            response->set_success(false);
            response->set_status_message("Falha ao comprimir PDF.");
            return Status::INTERNAL;
        }

        // Limpar arquivos temporários
        std::remove(input_file_path.c_str());
        std::remove(output_file_path.c_str());

        return Status::OK;
    }

    Status ConvertToTXT(ServerContext* context,
                        ServerReaderWriter<FileChunk, FileChunk>* stream) override {
        FileChunk chunk;
        std::string file_name;
        std::string input_file_path, output_file_path;
        std::ofstream input_file_stream;
        bool first_chunk = true;

        while (stream->Read(&chunk)) {
            if (first_chunk) {
                // Primeiro chunk deve conter JSON com metadados
                std::string metadata_json = chunk.content();
                // Exemplo simples de parse para extrair file_name (assumindo formato {"file_name":"nome.pdf"})
                size_t start_pos = metadata_json.find("\"file_name\":\"");
                if (start_pos == std::string::npos) {
                    LogError("ConvertToTXT", "", "Metadata inválida no primeiro chunk.");
                    return Status(grpc::INVALID_ARGUMENT, "Metadata inválida");
                }
                start_pos += strlen("\"file_name\":\"");
                size_t end_pos = metadata_json.find("\"", start_pos);
                if (end_pos == std::string::npos) {
                    LogError("ConvertToTXT", "", "Metadata inválida no primeiro chunk.");
                    return Status(grpc::INVALID_ARGUMENT, "Metadata inválida");
                }
                file_name = metadata_json.substr(start_pos, end_pos - start_pos);
                input_file_path = "/tmp/input_" + file_name;
                output_file_path = "/tmp/output_" + file_name + ".txt";

                input_file_stream.open(input_file_path, std::ios::binary);
                if (!input_file_stream) {
                    LogError("ConvertToTXT", file_name, "Falha ao criar arquivo temporário.");
                    return Status::INTERNAL;
                }
                first_chunk = false;
                continue;  // Pula esse chunk de metadados, não escreve arquivo
            }

            // Escrever bytes do PDF
            input_file_stream.write(chunk.content().c_str(), chunk.content().size());
        }

        input_file_stream.close();

        // Executar o pdftotext
        std::string command = "pdftotext " + input_file_path + " " + output_file_path;
        int result = std::system(command.c_str());

        if (result != 0) {
            LogError("ConvertToTXT", file_name, "Falha na conversão pdftotext");
            return Status::INTERNAL;
        }

        LogSuccess("ConvertToTXT", file_name, "Conversão PDF para TXT realizada.");

        // Enviar arquivo TXT de volta em chunks
        std::ifstream output_file_stream(output_file_path, std::ios::binary);
        if (!output_file_stream) {
            LogError("ConvertToTXT", file_name, "Erro ao abrir arquivo TXT para envio.");
            return Status::INTERNAL;
        }

        while (output_file_stream.peek() != EOF) {
            FileChunk response_chunk;
            char buffer[1024];
            output_file_stream.read(buffer, sizeof(buffer));
            response_chunk.set_content(buffer, output_file_stream.gcount());
            if (!stream->Write(response_chunk)) {
                // Cliente fechou conexão
                break;
            }
        }
        output_file_stream.close();

        // Limpar arquivos
        std::remove(input_file_path.c_str());
        std::remove(output_file_path.c_str());

        stream->WritesDone();
        return Status::OK;
    }

    Status ConvertImageFormat(ServerContext* context,
                         ServerReaderWriter<FileChunk, FileChunk>* stream) override {
    FileChunk chunk;
    std::string file_name;
    std::string output_format;
    std::string input_file_path, output_file_path;
    std::ofstream input_file_stream;
    bool first_chunk = true;

    while (stream->Read(&chunk)) {
        if (first_chunk) {
            // Parse JSON do primeiro chunk para extrair file_name e output_format
            std::string metadata_json = chunk.content();

            // Simples parse manual:
            size_t fn_pos = metadata_json.find("\"file_name\":\"");
            size_t of_pos = metadata_json.find("\"output_format\":\"");

            if (fn_pos == std::string::npos || of_pos == std::string::npos) {
                LogError("ConvertImageFormat", "", "Metadata inválida no primeiro chunk.");
                return Status(grpc::INVALID_ARGUMENT, "Metadata inválida");
            }

            fn_pos += strlen("\"file_name\":\"");
            size_t fn_end = metadata_json.find("\"", fn_pos);
            file_name = metadata_json.substr(fn_pos, fn_end - fn_pos);

            of_pos += strlen("\"output_format\":\"");
            size_t of_end = metadata_json.find("\"", of_pos);
            output_format = metadata_json.substr(of_pos, of_end - of_pos);

            input_file_path = "/tmp/input_" + file_name;
            output_file_path = "/tmp/output_" + file_name + "." + output_format;

            input_file_stream.open(input_file_path, std::ios::binary);
            if (!input_file_stream) {
                LogError("ConvertImageFormat", file_name, "Falha ao criar arquivo temporário.");
                return Status::INTERNAL;
            }

            first_chunk = false;
            continue; // pula o chunk de metadados
        }

        // Escreve bytes da imagem no arquivo temporário
        input_file_stream.write(chunk.content().c_str(), chunk.content().size());
    }

    input_file_stream.close();

    // Monta e executa o comando ImageMagick convert
    std::string command = "convert " + input_file_path + " " + output_file_path;
    int result = std::system(command.c_str());

    if (result != 0) {
        LogError("ConvertImageFormat", file_name, "Falha na conversão de imagem.");
        return Status::INTERNAL;
    }

    LogSuccess("ConvertImageFormat", file_name, "Conversão de formato de imagem realizada.");

    // Enviar arquivo convertido em chunks para o cliente
    std::ifstream output_file_stream(output_file_path, std::ios::binary);
    if (!output_file_stream) {
        LogError("ConvertImageFormat", file_name, "Erro ao abrir arquivo convertido para envio.");
        return Status::INTERNAL;
    }

    while (output_file_stream.peek() != EOF) {
        FileChunk response_chunk;
        char buffer[1024];
        output_file_stream.read(buffer, sizeof(buffer));
        response_chunk.set_content(buffer, output_file_stream.gcount());
        if (!stream->Write(response_chunk)) {
            break;  // Cliente desconectou
        }
    }
    output_file_stream.close();

    // Limpar arquivos temporários
    std::remove(input_file_path.c_str());
    std::remove(output_file_path.c_str());

    stream->WritesDone();
    return Status::OK;
}

    Status ResizeImage(ServerContext* context,
                    ServerReaderWriter<FileChunk, FileChunk>* stream) override {
        FileChunk chunk;
        std::string file_name;
        int width = 0;
        int height = 0;
        std::string input_file_path, output_file_path;
        std::ofstream input_file_stream;
        bool first_chunk = true;

        while (stream->Read(&chunk)) {
            if (first_chunk) {
                // Parse JSON do primeiro chunk para extrair file_name, width e height
                std::string metadata_json = chunk.content();

                // Exemplo de parsing simples (melhor usar uma lib JSON real em produção)
                size_t fn_pos = metadata_json.find("\"file_name\":\"");
                size_t w_pos = metadata_json.find("\"width\":");
                size_t h_pos = metadata_json.find("\"height\":");

                if (fn_pos == std::string::npos || w_pos == std::string::npos || h_pos == std::string::npos) {
                    LogError("ResizeImage", "", "Metadata inválida no primeiro chunk.");
                    return Status(grpc::INVALID_ARGUMENT, "Metadata inválida");
                }

                fn_pos += strlen("\"file_name\":\"");
                size_t fn_end = metadata_json.find("\"", fn_pos);
                file_name = metadata_json.substr(fn_pos, fn_end - fn_pos);

                w_pos += strlen("\"width\":");
                size_t w_end = metadata_json.find(",", w_pos);
                if (w_end == std::string::npos) w_end = metadata_json.find("}", w_pos);
                width = std::stoi(metadata_json.substr(w_pos, w_end - w_pos));

                h_pos += strlen("\"height\":");
                size_t h_end = metadata_json.find("}", h_pos);
                height = std::stoi(metadata_json.substr(h_pos, h_end - h_pos));

                input_file_path = "/tmp/input_" + file_name;
                output_file_path = "/tmp/output_" + file_name;

                input_file_stream.open(input_file_path, std::ios::binary);
                if (!input_file_stream) {
                    LogError("ResizeImage", file_name, "Falha ao criar arquivo temporário.");
                    return Status::INTERNAL;
                }

                first_chunk = false;
                continue;  // pular o chunk de metadados
            }

            // Escreve bytes da imagem no arquivo temporário
            input_file_stream.write(chunk.content().c_str(), chunk.content().size());
        }

        input_file_stream.close();

        // Executa o comando convert para redimensionar
        // Formato: convert input.jpg -resize 800x600 output.jpg
        std::string resize_param = std::to_string(width) + "x" + std::to_string(height);
        std::string command = "convert " + input_file_path + " -resize " + resize_param + " " + output_file_path;

        int result = std::system(command.c_str());
        if (result != 0) {
            LogError("ResizeImage", file_name, "Falha no redimensionamento de imagem.");
            return Status::INTERNAL;
        }

        LogSuccess("ResizeImage", file_name, "Redimensionamento de imagem realizado.");

        // Enviar arquivo redimensionado em chunks
        std::ifstream output_file_stream(output_file_path, std::ios::binary);
        if (!output_file_stream) {
            LogError("ResizeImage", file_name, "Erro ao abrir arquivo redimensionado para envio.");
            return Status::INTERNAL;
        }

        while (output_file_stream.peek() != EOF) {
            FileChunk response_chunk;
            char buffer[1024];
            output_file_stream.read(buffer, sizeof(buffer));
            response_chunk.set_content(buffer, output_file_stream.gcount());
            if (!stream->Write(response_chunk)) {
                break;  // cliente desconectou
            }
        }
        output_file_stream.close();

        // Limpar arquivos temporários
        std::remove(input_file_path.c_str());
        std::remove(output_file_path.c_str());

        stream->WritesDone();
        return Status::OK;
    }



private:
    void LogError(const std::string& service_name, const std::string& file_name, const std::string& message) {
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm now_tm;
        localtime_r(&now_c, &now_tm);
        char timestamp[26];
        std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &now_tm);

        std::ofstream log_file("server.log", std::ios::app);
        if (log_file.is_open()) {
            log_file << "[" << timestamp << "] ERROR - Service: " << service_name
                     << ", File: " << file_name << ", Message: " << message << std::endl;
            log_file.close();
        } else {
            std::cerr << "Falha ao abrir arquivo de log!" << std::endl;
        }

        std::cerr << "[" << timestamp << "] ERROR - Service: " << service_name << ", File: " << file_name << ", Message: " << message << std::endl; // Log para console também
    }

    void LogSuccess(const std::string& service_name, const std::string& file_name,
                    const std::string& message) {
        auto now = std::chrono::system_clock::now();
        std::time_t now_c = std::chrono::system_clock::to_time_t(now);
        std::tm now_tm;
        localtime_r(&now_c, &now_tm);
        char timestamp[26];
        std::strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", &now_tm);

        std::ofstream log_file("server.log", std::ios::app);
        if (log_file.is_open()) {
            log_file << "[" << timestamp << "] SUCCESS - Service: " << service_name
                     << ", File: " << file_name << ", Message: " << message << std::endl;
            log_file.close();
        } else {
            std::cerr << "Falha ao abrir arquivo de log!" << std::endl;
        }

        std::cout << "[" << timestamp << "] SUCCESS - Service: " << service_name
                  << ", File: " << file_name << ", Message: " << message << std::endl; // Log para console também
    }
};

void RunServer() {
    std::string server_address("0.0.0.0:50051");
    FileProcessorServiceImpl service;

    ServerBuilder builder;
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Servidor gRPC ouvindo em " << server_address << std::endl;

    server->Wait();
}

int main() {
    RunServer();
    return 0;
}
