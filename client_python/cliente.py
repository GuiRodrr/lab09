#!/usr/bin/env python3
# Cliente Python para FileProcessorService
import grpc
import file_processor_pb2
import file_processor_pb2_grpc
import sys
import os

CHUNK_SIZE = 64 * 1024  # 64KB

def compress_pdf(stub, input_file, output_file):
    """Compress PDF - RPC unário"""
    print(f"📦 Comprimindo PDF: {input_file} -> {output_file}")
    
    try:
        with open(input_file, "rb") as f:
            file_content = f.read()
        
        request = file_processor_pb2.FileRequest(
            file_name=os.path.basename(input_file),
            file_content=file_content
        )
        
        response = stub.CompressPDF(request)
        
        if response.success:
            with open(output_file, "wb") as f:
                f.write(response.file_content)
            print(f"✅ PDF comprimido com sucesso!")
            print(f"📊 Tamanho original: {len(file_content):,} bytes")
            print(f"📊 Tamanho comprimido: {len(response.file_content):,} bytes")
            print(f"📈 Redução: {((len(file_content) - len(response.file_content)) / len(file_content) * 100):.1f}%")
        else:
            print(f"❌ Erro na compressão: {response.status_message}")
            
    except Exception as e:
        print(f"❌ Erro ao comprimir PDF: {e}")

def file_chunk_iterator(file_path, **params):
    """Gerador de chunks para streaming"""
    base_name = os.path.basename(file_path)
    
    # Primeiro chunk com metadados
    first_chunk = file_processor_pb2.FileChunk()
    
    # Construir nome do arquivo com parâmetros se for resize ou conversão
    if 'width' in params and 'height' in params:
        first_chunk.file_name = f"{base_name}|{params['width']}|{params['height']}"
    elif 'format' in params:
        first_chunk.file_name = f"{base_name}|{params['format']}"
    else:
        first_chunk.file_name = base_name
    
    yield first_chunk
    
    # Chunks de dados
    with open(file_path, "rb") as f:
        while True:
            chunk_data = f.read(CHUNK_SIZE)
            if not chunk_data:
                break
            chunk = file_processor_pb2.FileChunk()
            chunk.chunk_data = chunk_data
            yield chunk
    
    # Último chunk
    last_chunk = file_processor_pb2.FileChunk()
    last_chunk.is_last = True
    yield last_chunk

def convert_to_txt(stub, input_file, output_file):
    """Convert to TXT - streaming bidirecional"""
    print(f"📝 Convertendo para TXT: {input_file} -> {output_file}")
    
    try:
        response_stream = stub.ConvertToTXT(
            file_chunk_iterator(input_file)
        )
        
        with open(output_file, "wb") as f:
            for response in response_stream:
                if response.chunk_data:
                    f.write(response.chunk_data)
        
        print(f"✅ Conversão para TXT concluída!")
        
    except Exception as e:
        print(f"❌ Erro na conversão para TXT: {e}")

def convert_image_format(stub, input_file, output_file, out_format):
    """Convert image format - streaming bidirecional"""
    print(f"🖼️ Convertendo imagem: {input_file} -> {output_file} ({out_format.upper()})")
    
    try:
        response_stream = stub.ConvertImageFormat(
            file_chunk_iterator(input_file, format=out_format)
        )
        
        with open(output_file, "wb") as f:
            for response in response_stream:
                if response.chunk_data:
                    f.write(response.chunk_data)
        
        print(f"✅ Conversão de formato concluída!")
        
    except Exception as e:
        print(f"❌ Erro na conversão de imagem: {e}")

def resize_image(stub, input_file, output_file, width, height):
    """Resize image - streaming bidirecional"""
    print(f"📐 Redimensionando imagem: {input_file} -> {output_file} ({width}x{height})")
    
    try:
        response_stream = stub.ResizeImage(
            file_chunk_iterator(input_file, width=width, height=height)
        )
        
        with open(output_file, "wb") as f:
            for response in response_stream:
                if response.chunk_data:
                    f.write(response.chunk_data)
        
        print(f"✅ Redimensionamento concluído!")
        
    except Exception as e:
        print(f"❌ Erro no redimensionamento: {e}")

def print_usage():
    print("🚀 Cliente File Processor gRPC")
    print("=" * 40)
    print("Uso:")
    print("  python client.py compress input.pdf output.pdf")
    print("  python client.py totxt input.pdf output.txt")
    print("  python client.py convertimg input.jpg output.png png")
    print("  python client.py resize input.jpg output.jpg 800 600")
    print("\nExemplos:")
    print("  python client.py compress document.pdf compressed.pdf")
    print("  python client.py totxt document.pdf output.txt")
    print("  python client.py convertimg image.jpg image.png png")
    print("  python client.py resize photo.jpg small_photo.jpg 300 200")

def main():
    if len(sys.argv) < 4:
        print_usage()
        sys.exit(1)

    cmd = sys.argv[1]
    input_file = sys.argv[2]
    output_file = sys.argv[3]

    # Verificar se arquivo de entrada existe
    if not os.path.exists(input_file):
        print(f"❌ Arquivo de entrada não encontrado: {input_file}")
        sys.exit(1)

    try:
        print("🔗 Conectando ao servidor gRPC...")
        channel = grpc.insecure_channel('localhost:50051')
        stub = file_processor_pb2_grpc.FileProcessorStub(channel)
        
        # Testar conexão
        try:
            channel.subscribe(lambda x: None, try_to_connect=True)
        except:
            print("❌ Servidor não está rodando! Execute ./server primeiro")
            sys.exit(1)

        if cmd == "compress":
            compress_pdf(stub, input_file, output_file)
        elif cmd == "totxt":
            convert_to_txt(stub, input_file, output_file)
        elif cmd == "convertimg":
            if len(sys.argv) < 5:
                print("❌ Precisa informar o formato de saída (ex: png, jpg)")
                sys.exit(1)
            out_format = sys.argv[4]
            convert_image_format(stub, input_file, output_file, out_format)
        elif cmd == "resize":
            if len(sys.argv) < 6:
                print("❌ Precisa informar width e height")
                sys.exit(1)
            width = int(sys.argv[4])
            height = int(sys.argv[5])
            resize_image(stub, input_file, output_file, width, height)
        else:
            print_usage()
            sys.exit(1)
            
    except grpc.RpcError as e:
        print(f"❌ Erro gRPC: {e.code()} - {e.details()}")
    except Exception as e:
        print(f"❌ Erro inesperado: {e}")

if __name__ == "__main__":
    main()