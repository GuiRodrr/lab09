import grpc
import file_processor_pb2
import file_processor_pb2_grpc

def compress_pdf(stub, input_file_path, output_file_path):
    def file_iterator():
        with open(input_file_path, 'rb') as f:
            while True:
                chunk = f.read(1024)
                if not chunk:
                    break
                yield file_processor_pb2.FileChunk(content=chunk)

    request = file_processor_pb2.FileRequest(
        file_name=input_file_path.split('/')[-1],  # nome do arquivo no request
        file_content=file_iterator(),
        compress_pdf_params=file_processor_pb2.CompressPDFRequest()
    )

    response_stream = stub.CompressPDF(request)
    try:
        with open(output_file_path, 'wb') as output_file:
            for chunk in response_stream:
                output_file.write(chunk.content)
        print(f"PDF comprimido e salvo em: {output_file_path}")
    except grpc.RpcError as e:
        print(f"Erro ao comprimir PDF: {e.details()}")
    except Exception as e:
        print(f"Erro ao salvar arquivo comprimido: {e}")

def run_client():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = file_processor_pb2_grpc.FileProcessorServiceStub(channel)
        input_pdf = "input.pdf"  # Substitua pelo caminho do seu arquivo PDF
        output_pdf = "compressed_output.pdf"
        compress_pdf(stub, input_pdf, output_pdf)

if __name__ == '__main__':
    run_client()
