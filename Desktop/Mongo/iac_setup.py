import os
import sys
import subprocess
import venv

# Colores para la consola
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
RESET = '\033[0m'

def print_step(message):
    print(f"\n{YELLOW}[*] {message}{RESET}")

def print_success(message):
    print(f"{GREEN}[+] {message}{RESET}")

def print_error(message):
    print(f"{RED}[-] {message}{RESET}")
    sys.exit(1)

def check_python_version():
    print_step("Validando versión de Python...")
    if sys.version_info < (3, 8):
        print_error("Se requiere Python 3.8 o superior.")
    print_success(f"Python versión {sys.version_info.major}.{sys.version_info.minor} detectada.")

def setup_virtualenv():
    venv_dir = ".venv"
    print_step(f"Configurando entorno virtual en '{venv_dir}'...")
    if not os.path.exists(venv_dir):
        venv.create(venv_dir, with_pip=True)
        print_success("Entorno virtual creado exitosamente.")
    else:
        print_success("El entorno virtual ya existe.")
    return venv_dir

def install_dependencies(venv_dir):
    print_step("Instalando dependencias desde requirements.txt...")
    if not os.path.exists("requirements.txt"):
        print_error("No se encontró el archivo requirements.txt. Asegúrate de crearlo con pandas, pymongo, pyspark, etc.")
    
    # Usar el pip del entorno virtual recién creado
    if os.name == 'nt':
        pip_exe = os.path.join(venv_dir, "Scripts", "pip.exe")
    else:
        pip_exe = os.path.join(venv_dir, "bin", "pip")
    
    try:
        subprocess.check_call([pip_exe, "install", "-r", "requirements.txt"])
        print_success("Dependencias instaladas correctamente.")
    except subprocess.CalledProcessError:
        print_error("Error al instalar las dependencias.")

def provision_data_lake():
    print_step("Provisionando estructura del Data Lake...")
    folders = [
        "data_lake/raw",
        "data_lake/processed",
        "data_lake/curated"
    ]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        print_success(f"Directorio listo: {folder}")

def health_check(venv_dir):
    print_step("Realizando Health Check de configuración...")
    if not os.path.exists(".env"):
        print_error("El archivo .env no existe. Por favor créalo con tu URI de MongoDB.")
    print_success("Archivo .env encontrado.")

    # Usar el Python del entorno virtual para probar la conexión a Atlas
    if os.name == 'nt':
        python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
    else:
        python_exe = os.path.join(venv_dir, "bin", "python")
        
    mongo_check_script = """
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()
uri = os.getenv('MONGO_URI') # Asegúrate de que la variable en tu .env se llame así
if not uri:
    print('[-] Variable MONGO_URI no definida en el .env')
    exit(1)
try:
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    print('[+] Ping a MongoDB Atlas exitoso!')
except Exception as e:
    print(f'[-] Error conectando a MongoDB: {e}')
    exit(1)
"""
    print_step("Probando conexión a MongoDB Atlas...")
    try:
        subprocess.check_call([python_exe, "-c", mongo_check_script])
    except subprocess.CalledProcessError:
        print_error("El Health Check de MongoDB falló. Revisa tu conexión o credenciales en el .env.")

def main():
    print(f"{GREEN}\n🚀 INICIANDO ORQUESTADOR DE INFRAESTRUCTURA (IaC SETUP) 🚀{RESET}")
    check_python_version()
    venv_dir = setup_virtualenv()
    install_dependencies(venv_dir)
    provision_data_lake()
    health_check(venv_dir)
    
    print(f"\n✨ {GREEN}¡Configuración completada con éxito!{RESET}")
    print("👉 Para activar el entorno virtual, ejecuta:")
    if os.name == 'nt':
        print("   .venv\\Scripts\\activate\n")
    else:
        print("   source .venv/bin/activate\n")

if __name__ == "__main__":
    main()