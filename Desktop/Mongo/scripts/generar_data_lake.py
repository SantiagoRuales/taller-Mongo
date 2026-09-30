import os
import pymongo
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
from bson import ObjectId  # <-- NUEVO: Necesario para reconocer los ObjectIds de Mongo

# ==========================================
# 1. CONFIGURACIÓN Y RUTAS
# ==========================================
ruta_env = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(ruta_env)
MONGO_URI = os.getenv("MONGO_URI")

DATA_LAKE_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_lake")
ZONAS = {
    "raw": os.path.join(DATA_LAKE_ROOT, "raw"),
    "processed": os.path.join(DATA_LAKE_ROOT, "processed"),
    "curated": os.path.join(DATA_LAKE_ROOT, "curated")
}

# ==========================================
# NUEVO: FUNCIÓN DE LIMPIEZA PROFUNDA
# ==========================================
def limpiar_objectids(dato):
    """
    Escanea todo el documento (incluyendo listas anidadas como 'items')
    y convierte cualquier ObjectId en texto puro para evitar errores en Parquet.
    """
    if isinstance(dato, dict):
        return {k: limpiar_objectids(v) for k, v in dato.items()}
    elif isinstance(dato, list):
        return [limpiar_objectids(i) for i in dato]
    elif isinstance(dato, ObjectId):
        return str(dato)
    return dato

# ==========================================
# 2. EL PIPELINE ETL PRINCIPAL
# ==========================================
def ejecutar_etl():
    print("🚀 Iniciando Pipeline ETL (Fase 2)...")
    
    client = pymongo.MongoClient(MONGO_URI)
    db = client["ventas_db"]

    # ------------------------------------------
    # ETAPA 1: RAW (Extracción cruda)
    # ------------------------------------------
    print("\n[1/3] Ejecutando Etapa RAW...")
    datos_raw = list(db["ventas"].find({}))
    
    if not datos_raw:
        print("⚠️ No hay datos en MongoDB. ¡Deteniendo el pipeline!")
        return

    # APLICAMOS LA LIMPIEZA PROFUNDA ANTES DE PASARLO A PANDAS
    datos_limpios = limpiar_objectids(datos_raw)
    df_raw = pd.json_normalize(datos_limpios)
        
    ruta_raw = os.path.join(ZONAS["raw"], "ventas.parquet")
    df_raw.to_parquet(ruta_raw, compression="snappy", index=False)
    print("  ✔️ Exportado: ventas.parquet")

    # ------------------------------------------
    # ETAPA 2: PROCESSED (Transformaciones)
    # ------------------------------------------
    print("\n[2/3] Ejecutando Etapa PROCESSED...")
    
    df_processed = pd.read_parquet(ruta_raw)

    if "estado" in df_processed.columns:
        df_processed = df_processed[df_processed["estado"] == "Completada"].copy()
    
    if "fecha" in df_processed.columns:
        df_processed["fecha"] = pd.to_datetime(df_processed["fecha"], errors='coerce')
        df_processed["anio"] = df_processed["fecha"].dt.year
        df_processed["mes"] = df_processed["fecha"].dt.month
    
    df_processed["fecha_procesamiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ruta_processed = os.path.join(ZONAS["processed"], "ventas_procesadas.parquet")
    df_processed.to_parquet(ruta_processed, compression="snappy", index=False)
    print("  ✔️ Transformación lista: ventas_procesadas.parquet")

    # ------------------------------------------
    # ETAPA 3: CURATED (Modelo Estrella para BI)
    # ------------------------------------------
    print("\n[3/3] Ejecutando Etapa CURATED...")
    
    df_curated = pd.read_parquet(ruta_processed)

    columnas_deseadas = ["_id", "cliente_id", "producto_id", "total_venta", "anio", "mes", "fecha_procesamiento"]
    columnas_finales = [col for col in columnas_deseadas if col in df_curated.columns]
    
    df_fact_ventas = df_curated[columnas_finales]
    
    ruta_curated = os.path.join(ZONAS["curated"], "fact_ventas.parquet")
    df_fact_ventas.to_parquet(ruta_curated, compression="snappy", index=False)
    print("  ✔️ Modelo analítico listo: fact_ventas.parquet")

    # ------------------------------------------
    # RESUMEN 
    # ------------------------------------------
    print("\n📊 Resumen del Data Lake:")
    total = 0
    for nombre, ruta in ZONAS.items():
        archivos = [f for f in os.listdir(ruta) if f.endswith('.parquet')]
        total += len(archivos)
        print(f"  - {nombre.upper()}: {len(archivos)} archivo(s)")
    print(f"  TOTAL: {total} archivo(s) Parquet generados.")

if __name__ == "__main__":
    ejecutar_etl()