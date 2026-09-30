import os
import pymongo
from dotenv import load_dotenv
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")

def conectar_mongodb():
    try:
        print("🔄 Intentando conectar a MongoDB Atlas...")
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✅ ¡Conexión exitosa a MongoDB Atlas!")

        db = client["ventas_db"]
        return client, db

    except ConnectionFailure:
        print("❌ Error: No se pudo contactar al servidor.")
    except ConfigurationError as e:
        print(f"❌ Error de configuración: {e}")
    except OperationFailure as e:
        print(f"❌ Error de autenticación: {e}")

    return None, None

if __name__ == "__main__":
    client, db = conectar_mongodb()
    if client:
        client.close()
        print("🔒 Conexión cerrada.")