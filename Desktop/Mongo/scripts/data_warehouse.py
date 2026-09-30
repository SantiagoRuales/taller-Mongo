import os
import pymongo
from dotenv import load_dotenv
import time

# ==========================================
# Paso 1 y 2: Importar librerías y Conexión
# ==========================================
ruta_env = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(ruta_env)
MONGO_URI = os.getenv("MONGO_URI")

def construir_data_warehouse():
    print("🏗️ Iniciando construcción del Data Warehouse (Modelo Estrella)...")
    tiempo_inicio = time.time()

    # Crear la conexión
    client = pymongo.MongoClient(MONGO_URI)
    db = client["ventas_db"]
    
    # Fuente de todo: Tu colección de ventas original
    coleccion_origen = db["ventas"] 
    
    # Limpieza inicial: Empezar desde cero eliminando dimensiones anteriores
    db.dim_tiempo.drop()
    db.dim_cliente.drop()
    db.dim_producto.drop()
    db.fact_ventas.drop()

    # ==========================================
    # Paso 3: Crear la dimensión Tiempo (dim_tiempo)
    # ==========================================
    print("\n⏳ Construyendo dim_tiempo...")
    pipeline_tiempo = [
        {"$match": {"fecha_venta": {"$exists": True, "$ne": None}}},
        {"$group": {
            "_id": "$fecha_venta",
            "fecha_original": {"$first": "$fecha_venta"}
        }},
        {"$project": {
            "_id": 0,
            "fecha_id": "$_id",
            "fecha": {"$toDate": "$fecha_original"},
            "anio": {"$year": {"$toDate": "$fecha_original"}},
            "mes": {"$month": {"$toDate": "$fecha_original"}},
            "dia": {"$dayOfMonth": {"$toDate": "$fecha_original"}},
            "trimestre": {"$ceil": {"$divide": [{"$month": {"$toDate": "$fecha_original"}}, 3]}},
            "dia_semana": {"$dayOfWeek": {"$toDate": "$fecha_original"}}
        }},
        {"$merge": {"into": "dim_tiempo", "whenMatched": "replace", "whenNotMatched": "insert"}}
    ]
    coleccion_origen.aggregate(pipeline_tiempo, allowDiskUse=True)

    # ==========================================
    # Paso 4: Crear la dimensión Cliente (dim_cliente)
    # ==========================================
    print("👤 Construyendo dim_cliente...")
    pipeline_cliente = [
        {"$match": {"cliente_id": {"$exists": True}}},
        {"$group": {
            "_id": "$cliente_id",
            "estado_ejemplo": {"$first": "$estado"}, 
            "total_comprado": {"$sum": "$total"},
            # CORRECCIÓN AQUÍ: Solo sacamos el promedio bruto en el $group
            "ticket_promedio_bruto": {"$avg": "$total"}
        }},
        {"$project": {
            "_id": 0,
            "cliente_id": "$_id",
            "estado_ejemplo": 1,
            "total_comprado": 1,
            # CORRECCIÓN AQUÍ: Hacemos el redondeo en el $project
            "ticket_promedio": {"$round": ["$ticket_promedio_bruto", 2]}
        }},
        {"$merge": {"into": "dim_cliente", "whenMatched": "replace", "whenNotMatched": "insert"}}
    ]
    coleccion_origen.aggregate(pipeline_cliente, allowDiskUse=True)

    # ==========================================
    # Paso 5: Crear la dimensión Producto (dim_producto)
    # ==========================================
    print("📦 Construyendo dim_producto...")
    pipeline_producto = [
        {"$unwind": {"path": "$items", "preserveNullAndEmptyArrays": False}},
        {"$group": {
            "_id": "$items", 
            "unidades_vendidas": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "producto_id": "$_id",
            "unidades_vendidas": 1
        }},
        {"$merge": {"into": "dim_producto", "whenMatched": "replace", "whenNotMatched": "insert"}}
    ]
    coleccion_origen.aggregate(pipeline_producto, allowDiskUse=True)

    # ==========================================
    # Paso 6: Crear la Tabla de Hechos (fact_ventas)
    # ==========================================
    print("📊 Construyendo fact_ventas...")
    pipeline_fact = [
        {"$project": {
            "_id": 0,
            "venta_id": "$_id",
            "fecha_id": "$fecha_venta",
            "cliente_id": "$cliente_id",
            "metodo_pago": "$metodo_pago",
            "total": "$total"
        }},
        {"$merge": {"into": "fact_ventas", "whenMatched": "replace", "whenNotMatched": "insert"}}
    ]
    coleccion_origen.aggregate(pipeline_fact, allowDiskUse=True)

    # ==========================================
    # Paso 8: El resumen final
    # ==========================================
    print("\n✅ DATA WAREHOUSE CONFIGURADO CON ÉXITO")
    print("Resumen de registros en cada dimensión:")
    print(f"  - dim_tiempo: {db.dim_tiempo.count_documents({})} registros")
    print(f"  - dim_cliente: {db.dim_cliente.count_documents({})} registros")
    print(f"  - dim_producto: {db.dim_producto.count_documents({})} registros")
    print(f"  - fact_ventas: {db.fact_ventas.count_documents({})} registros")
    
    tiempo_fin = time.time()
    print(f"\n⏱️ Tiempo total de ejecución: {round(tiempo_fin - tiempo_inicio, 2)} segundos.")

if __name__ == "__main__":
    construir_data_warehouse()