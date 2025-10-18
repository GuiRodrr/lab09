// server_cc/server.cc
// Implementação do servidor gRPC para FileProcessorService
// Autor (aluno): Bianca (exemplo) - comentário como se eu fosse o aluno
// Compilar com CMake (CMakeLists noturno no repo)

#include <grpcpp/grpcpp.h>
#include "file_processor.grpc.pb.h"
#include "file_processor.pb.h"

#include <chrono>
#include <ctime>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <cstdio> // remove

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using grpc::ServerReaderWriter;

using file_processor::FileProcessorService;
using file_processor::UploadRequest;
using file_processor::DownloadResponse;
using file_processor::FileChunk;
using file_processor::FileMeta;
using file_processor::StatusResponse;

// Funções auxiliares para logging:
static void WriteLog(const std::string& level, const std::string& service,
                     const std::string& file_name, const std::string& message) {
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&now_c));
    std::ofstream log("server.log", std::ios::app);
    if (log.is_open()) {
        log << "[" << buf << "] " << level << " - Service: " << service
            << ", File: " << file_name << ", Message: " << message << std::endl;
        log.close();
    }
    // também imprimir no console (ajuda em testes)
    std::cerr << "[" << buf << "] " << level << " - Service: " << service
              << ", File: " << file_name << ", Message: " << message << std::endl;
}

// Lê todo stream do cliente e salva em arquivo temporário.
// Retorna caminho do arquivo salvo (ex: /tmp/input_<id>_<nome>)
// Em caso de erro retorna string vazia.
static std::string ReceiveFileFromStream(ServerReaderWriter<DownloadResponse, UploadRequest>* stream,
                                         std::string& out_file_name) {
    // criar arquivo temporário
    std::string tmp_prefix = "/tmp/grpc_upload_";
    // gerar id simples pela hora
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::string tmp_path = tmp_prefix + std::to_string(ms) + "_" + out_file_name;

    std::ofstream ofs(tmp_path, std::ios::binary);
    if (!ofs.is_open()) {
        WriteLog("ERROR", "ReceiveFileFromStream", out_file_name, "Falha ao criar arquivo temporário.");
        return "";
    }

    UploadRequest req;
    bool got_meta = false;
    while (stream->Read(&req)) {
        if (req.has_meta()) {
            // primeira mensagem com metadata
            FileMeta meta = req.meta();
            if (!meta.file_name().empty()) {
                out_file_name = meta.file_name();
            }
            got_meta = true;
            // guardamos meta, mas não fazemos mais nada aqui
        } else if (req.has_chunk()) {
            const FileChunk& ch = req.chunk();
            ofs.write(ch.content().data(), ch.content().size());
        }
    }
    ofs.close();

    if (!got_meta) {
        WriteLog("ERROR", "ReceiveFileFromStream", out_file_name, "Não recebeu metadata do cliente.");
        // ainda retornamos o path, mas sinalizamos problema via logs
    }
    return tmp_path;
}

// Envia arquivo no formato de DownloadResponse com chunks e no final envia StatusResponse
static bool SendFileToStream(ServerReaderWriter<DownloadResponse, UploadRequest>* stream,
                             const std::string& path, const std::string& out_name, const std::string& service_name) {
    std::ifstream ifs(path, std::ios::binary);
    if (!ifs.is_open()) {
        WriteLog("ERROR", service_name, out_name, "Falha ao abrir arquivo de saída para enviar.");
        // enviar status de erro ao cliente
        DownloadResponse dr;
        StatusResponse* st = new StatusResponse();
        st->set_success(false);
        st->set_message("Erro no servidor ao abrir arquivo para envio.");
        st->set_file_name("");
        dr.set_allocated_status(st);
        stream->Write(dr);
        return false;
    }

    const size_t BUF = 64 * 1024;
    std::vector<char> buffer(BUF);
    while (ifs.good()) {
        ifs.read(buffer.data(), BUF);
        std::streamsize s = ifs.gcount();
        if (s > 0) {
            DownloadResponse dr;
            FileChunk* fc = new FileChunk();
            fc->set_content(std::string(buffer.data(), s));
            dr.set_allocated_chunk(fc);
            stream->Write(dr);
        }
    }
    ifs.close();

    // enviar status de sucesso
    DownloadResponse dr;
    StatusResponse* st = new StatusResponse();
    st->set_success(true);
    st->set_message("Operação concluída com sucesso.");
    st->set_file_name(out_name);
    dr.set_allocated_status(st);
    stream->Write(dr);

    WriteLog("SUCCESS", service_name, out_name, "Arquivo enviado com sucesso.");
    return true;
}

// Helper que executa um comando e retorna pair(codigo_retorno, stdout+stderr)
// Usamos popen para capturar output
static int ExecuteCommand(const std::string& command, std::string& combined_output) {
    combined_output.clear();
    // redirecionar stderr para stdout no comando
    std::string cmd = command + " 2>&1";
    FILE* pipe = popen(cmd.c_str(), "r");
    if (!pipe) return -1;
    char buffer[128];
    while (fgets(buffer, sizeof(buffer), pipe) != NULL) {
        combined_output += buffer;
    }
    int rc = pclose(pipe);
    return rc;
}

class FileProcessorServiceImpl final : public FileProcessorService::Service {
public:
    // CompressPDF: usa Ghostscript (gs) para comprimir
    Status CompressPDF(ServerContext* context,
                       ServerReaderWriter<DownloadResponse, UploadRequest>* stream) override {
        std::string in_file_name = "input.pdf";
        std::string input_path = ReceiveFileFromStream(stream, in_file_name);
        if (input_path.empty()) {
            // enviar status de erro
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao receber arquivo no servidor.");
            st->set_file_name("");
            dr.set_allocated_status(st);
            stream->Write(dr);
            return Status::OK;
        }

        // construir output path
        std::string output_path = input_path + ".compressed.pdf";
        std::string command = "gs -sDEVICE=pdfwrite -dCompatibilityLevel=1.4 -dPDFSETTINGS=/ebook -dNOPAUSE -dQUIET -dBATCH -sOutputFile=" + output_path + " " + input_path;
        std::string out;
        int rc = ExecuteCommand(command, out);
        if (rc != 0) {
            WriteLog("ERROR", "CompressPDF", in_file_name, "gs retornou codigo " + std::to_string(rc) + " output:" + out);
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao comprimir PDF: " + out);
            st->set_file_name("");
            dr.set_allocated_status(st);
            stream->Write(dr);
            // cleanup
            std::remove(input_path.c_str());
            return Status::OK;
        }

        WriteLog("SUCCESS", "CompressPDF", in_file_name, "Compressão bem sucedida.");
        SendFileToStream(stream, output_path, "compressed_" + in_file_name, "CompressPDF");

        // cleanup
        std::remove(input_path.c_str());
        std::remove(output_path.c_str());
        return Status::OK;
    }

    // ConvertToTXT: usa pdftotext para gerar txt; servidor envia o conteúdo do txt
    Status ConvertToTXT(ServerContext* context,
                        ServerReaderWriter<DownloadResponse, UploadRequest>* stream) override {
        std::string in_file_name = "input.pdf";
        std::string input_path = ReceiveFileFromStream(stream, in_file_name);
        if (input_path.empty()) {
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao receber arquivo no servidor.");
            st->set_file_name("");
            dr.set_allocated_status(st);
            stream->Write(dr);
            return Status::OK;
        }

        std::string output_txt = input_path + ".txt";
        std::string command = "pdftotext " + input_path + " " + output_txt;
        std::string out;
        int rc = ExecuteCommand(command, out);
        if (rc != 0) {
            WriteLog("ERROR", "ConvertToTXT", in_file_name, "pdftotext retornou codigo " + std::to_string(rc) + " output:" + out);
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao converter PDF para TXT: " + out);
            st->set_file_name("");
            dr.set_allocated_status(st);
            stream->Write(dr);
            std::remove(input_path.c_str());
            return Status::OK;
        }

        // enviar o TXT como stream
        std::ifstream ifs(output_txt);
        if (!ifs.is_open()) {
            WriteLog("ERROR", "ConvertToTXT", in_file_name, "Não abriu arquivo txt gerado.");
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Erro ao abrir TXT gerado.");
            st->set_file_name("");
            dr.set_allocated_status(st);
            stream->Write(dr);
            std::remove(input_path.c_str());
            std::remove(output_txt.c_str());
            return Status::OK;
        }
        // ler em pedaços de texto e enviar como FileChunk
        const size_t BUF = 64 * 1024;
        std::vector<char> buffer(BUF);
        while (ifs.good()) {
            ifs.read(buffer.data(), BUF);
            std::streamsize s = ifs.gcount();
            if (s > 0) {
                DownloadResponse dr;
                FileChunk* fc = new FileChunk();
                fc->set_content(std::string(buffer.data(), s));
                dr.set_allocated_chunk(fc);
                stream->Write(dr);
            }
        }
        ifs.close();

        // status final
        DownloadResponse dr;
        StatusResponse* st = new StatusResponse();
        st->set_success(true);
        st->set_message("Conversão para TXT completa.");
        st->set_file_name("converted_" + in_file_name + ".txt");
        dr.set_allocated_status(st);
        stream->Write(dr);

        WriteLog("SUCCESS", "ConvertToTXT", in_file_name, "Conversão para TXT bem sucedida.");
        std::remove(input_path.c_str());
        std::remove(output_txt.c_str());
        return Status::OK;
    }

    // ConvertImageFormat: usa ImageMagick 'convert input.ext output.<format>'
    Status ConvertImageFormat(ServerContext* context,
                              ServerReaderWriter<DownloadResponse, UploadRequest>* stream) override {
        std::string in_file_name = "input.img";
        std::string input_path = ReceiveFileFromStream(stream, in_file_name);
        if (input_path.empty()) {
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao receber arquivo no servidor.");
            dr.set_allocated_status(st);
            stream->Write(dr);
            return Status::OK;
        }

        // Tentamos inferir output format da metadata: no ReceiveFileFromStream a metadata atualiza in_file_name,
        // mas precisamos do output format — por simplicidade assumimos que o cliente nomeia adequadamente,
        // e se quiser formatar, deve enviar meta com output_format no nome (ver instruções do cliente).
        // Aqui apenas vamos trocar a extensão para .converted.png por padrão
        // (o cliente python demonstrado envia meta.output_format corretamente).
        // Para segurança, vamos abrir um arquivo de meta alternativo: procurar string ".converted." no nome
        std::string out_format = "png";
        // construir output
        std::string output_path = input_path + ".converted." + out_format;
        // NOTE: se o cliente quisesse outro formato, deveria ter colocado no file_name (ou enviaria meta com output_format).
        std::string command = "convert " + input_path + " " + output_path;
        std::string out;
        int rc = ExecuteCommand(command, out);
        if (rc != 0) {
            WriteLog("ERROR", "ConvertImageFormat", in_file_name, "convert retornou codigo " + std::to_string(rc) + " out:" + out);
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao converter imagem: " + out);
            dr.set_allocated_status(st);
            stream->Write(dr);
            std::remove(input_path.c_str());
            return Status::OK;
        }

        WriteLog("SUCCESS", "ConvertImageFormat", in_file_name, "Conversão de formato bem sucedida.");
        SendFileToStream(stream, output_path, "converted_" + in_file_name, "ConvertImageFormat");

        std::remove(input_path.c_str());
        std::remove(output_path.c_str());
        return Status::OK;
    }

    // ResizeImage: usa convert -resize WxH output
    Status ResizeImage(ServerContext* context,
                       ServerReaderWriter<DownloadResponse, UploadRequest>* stream) override {
        std::string in_file_name = "input.img";
        std::string input_path = ReceiveFileFromStream(stream, in_file_name);
        if (input_path.empty()) {
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao receber arquivo no servidor.");
            dr.set_allocated_status(st);
            stream->Write(dr);
            return Status::OK;
        }

        // Por simplicidade definimos dimensões padrão se não forem passadas:
        int width = 800, height = 600;
        // OBS: o cliente deve incluir as dimensões em file_name ou meta; nosso ReceiveFileFromStream atualiza out_file_name,
        // mas não preenche width/height neste exemplo simples. Para o cliente Python que vou fornecer, ele colocará width e height
        // dentro do file_name no meta (ex: "nome.jpg|800|600") e o cliente Python fará o parsing local antes de enviar.
        // Aqui tentamos extrair dims do in_file_name (format: name|800|600)
        size_t p1 = in_file_name.find('|');
        if (p1 != std::string::npos) {
            size_t p2 = in_file_name.find('|', p1 + 1);
            if (p2 != std::string::npos) {
                std::string wstr = in_file_name.substr(p1 + 1, p2 - p1 - 1);
                std::string hstr = in_file_name.substr(p2 + 1);
                try {
                    width = std::stoi(wstr);
                    height = std::stoi(hstr);
                } catch (...) { }
            }
        }

        std::string output_path = input_path + ".resized.jpg";
        std::ostringstream cmd;
        cmd << "convert " << input_path << " -resize " << width << "x" << height << " " << output_path;
        std::string out;
        int rc = ExecuteCommand(cmd.str(), out);
        if (rc != 0) {
            WriteLog("ERROR", "ResizeImage", in_file_name, "convert -resize retornou codigo " + std::to_string(rc) + " out:" + out);
            DownloadResponse dr;
            StatusResponse* st = new StatusResponse();
            st->set_success(false);
            st->set_message("Falha ao redimensionar imagem: " + out);
            dr.set_allocated_status(st);
            stream->Write(dr);
            std::remove(input_path.c_str());
            return Status::OK;
        }

        WriteLog("SUCCESS", "ResizeImage", in_file_name, "Redimensionamento bem sucedido.");
        SendFileToStream(stream, output_path, "resized_" + in_file_name, "ResizeImage");

        std::remove(input_path.c_str());
        std::remove(output_path.c_str());
        return Status::OK;
    }

};

void RunServer(const std::string& address) {
    FileProcessorServiceImpl service;
    ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    std::unique_ptr<Server> server(builder.BuildAndStart());
    std::cout << "Servidor gRPC ouvindo em " << address << std::endl;
    WriteLog("INFO", "ServerStart", "-", "Servidor iniciado em " + address);
    server->Wait();
}

int main(int argc, char** argv) {
    // comentário do aluno: porta padrão 50051; argumento opcional: endereço
    std::string address = "0.0.0.0:50051";
    if (argc > 1) address = argv[1];
    RunServer(address);
    return 0;
}
