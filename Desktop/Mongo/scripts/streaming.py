import os
import pymongo
import time
import json
from dotenv import load_dotenv
from datetime import datetime

# ==========================================
# BLOQUE 2: Cargar la conexión
# ==========================================
ruta_env = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(ruta_env)
MONGO_URI = os.getenv("MONGO_URI")

# ==========================================
# BLOQUES 3 AL 12: Función principal de Streaming
# ==========================================
def simular_streaming_kafka():
    print("🌊 Iniciando pipeline de streaming (Simulación Kafka)...")
    print("🎧 MongoDB Change Streams escuchando la colección 'ventas'...\n")
    
    # Bloque 4: Conectarse a MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    db = client["ventas_db"]
    
    contador = 0

    try:
        # Bloque 5: Change Streams (El 'Consumer' de Kafka)
        # watch() crea un cursor que nunca termina y escucha continuamente
        with db.ventas.watch() as stream:
            
            # Bloque 6: Escuchar eventos
            for change in stream:
                contador += 1
                hora_actual = datetime.now().strftime("[%H:%M:%S]")
                
                # Bloque 7: Identificar el tipo de operación
                operacion = change.get("operationType")
                
                print(f"{hora_actual} EVENTO DETECTADO: {operacion.upper()}")

                if operacion == "insert":
                    # Extraer el documento insertado
                    documento = change.get("fullDocument", {})
                    factura = documento.get("numero_factura", "N/A")
                    total = documento.get("total_venta", 0) # Asumo 'total_venta' por scripts anteriores
                    
                    print(f"  -> Nueva Venta | Factura: {factura} | Total: ${total}")
                    
                    # Bloque 8: Alertas en tiempo real (Regla de negocio)
                    if total > 10000000:
                        print("  ⚠️ ALERTA: ¡Venta de alto valor detectada!")
                
                # Bloque 9: Update
                elif operacion == "update":
                    id_afectado = change.get("documentKey", {}).get("_id")
                    print(f"  -> Documento Actualizado | ID: {id_afectado}")
                
                # Bloque 10: Delete (Útil para auditoría)
                elif operacion == "delete":
                    id_afectado = change.get("documentKey", {}).get("_id")
                    print(f"  -> Documento Eliminado | ID: {id_afectado}")

                # Bloque 11: Procesamiento cada diez eventos (Simulando micro-batch de Spark)
                if contador % 10 == 0:
                    print(f"\n✅ SIMULACIÓN SPARK: Procesados {contador} eventos en este micro-batch.\n")

    # Bloque 12: Detener el stream de forma segura
    except KeyboardInterrupt:
        print("\n🛑 Stream cerrado por el usuario (Ctrl+C).")
    finally:
        client.close()
        print("🔌 Conexión a MongoDB cerrada correctamente.")

# ==========================================
# BLOQUE 13: Punto de entrada
# ==========================================
if __name__ == "__main__":
    print("""
    INSTRUCCIONES PARA LA PRÁCTICA:
    1. Deja esta terminal (Terminal 1) abierta y corriendo.
    2. Abre una SEGUNDA terminal (Terminal 2).
    3. En la Terminal 2, ejecuta tu script que inserta datos (ej: generar_datos_masivos.py o generar_ventas_streaming.py).
    4. Observa cómo aparecen los eventos en tiempo real aquí.
    """)
    simular_streaming_kafka()