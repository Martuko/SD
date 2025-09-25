#!/usr/bin/env python3
import sys
import time
import socket
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

def check_kafka_ready(host, port, max_retries=30):
    """Verificar que Kafka esté completamente listo"""
    
    print(f"Verificando Kafka en {host}:{port}...")
    
    # 1. Verificar que el puerto esté abierto
    for i in range(max_retries):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                result = sock.connect_ex((host, port))
                if result == 0:
                    print(" Puerto de Kafka está abierto")
                    break
        except Exception as e:
            print(f"Error de socket: {e}")
        
        if i == max_retries - 1:
            print(" ERROR: Puerto de Kafka nunca estuvo disponible")
            return False
        
        print(f"Esperando puerto... intento {i+1}/{max_retries}")
        time.sleep(3)
    
    # 2. Verificar que Kafka responda a comandos administrativos
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=f"{host}:{port}",
                request_timeout_ms=10000,
            )
            topics = admin.list_topics()
            admin.close()
            print(" Kafka está completamente operativo")
            return True
        except NoBrokersAvailable:
            print(f" Kafka no listo... intento {i+1}/{max_retries}")
        except Exception as e:
            print(f" Error de conexión Kafka: {e}")
        
        time.sleep(3)
    
    print("ERROR: Kafka no está operativo después de todos los intentos")
    return False

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Uso: python wait_for_kafka.py <host> <port> <comando>")
        sys.exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2])
    command = sys.argv[3:]
    
    if check_kafka_ready(host, port):
        print(f" Ejecutando: {' '.join(command)}")
        import subprocess
        sys.exit(subprocess.run(command).returncode)
    else:
        print(" No se pudo iniciar la aplicación")
        sys.exit(1)