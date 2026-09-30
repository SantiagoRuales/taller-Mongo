import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

# ==========================================
# Configuración de Entorno
# ==========================================
os.environ['PYSPARK_SUBMIT_ARGS'] = '--master local[*] pyspark-shell'
os.environ['SPARK_LOCAL_HOSTNAME'] = 'localhost'

DATA_LAKE_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_lake")
ZONA_PROCESSED = os.path.join(DATA_LAKE_ROOT, "processed")
ZONA_CURATED = os.path.join(DATA_LAKE_ROOT, "curated")
os.makedirs(ZONA_CURATED, exist_ok=True)

# ==========================================
# Funciones Auxiliares
# ==========================================
def obtener_archivo_parquet(ruta_carpeta):
    """Busca el archivo Parquet más reciente en una carpeta."""
    patron = os.path.join(ruta_carpeta, "*.parquet")
    archivos = glob.glob(patron)
    if not archivos:
        raise FileNotFoundError(f"No se encontraron archivos Parquet en: {ruta_carpeta}")
    return max(archivos, key=os.path.getctime)

# ==========================================
# Definición de los Spark Jobs
# ==========================================
def job1_ventas_metodo_pago(spark, ruta_origen):
    print("\n--- Ejecutando Job 1: Ventas por Método de Pago ---")
    df = spark.read.parquet(ruta_origen)
    
    if "metodo_pago" not in df.columns:
        print("⚠️ Advertencia: La columna 'metodo_pago' no existe. Saltando Job 1.")
        return

    # CORRECCIÓN AQUÍ: Cambiamos 'total_venta' por 'total'
    resultado = df.groupBy("metodo_pago").agg(
        F.count("*").alias("cantidad_ventas"),
        F.sum("total").alias("ingresos_totales"), 
        F.avg("total").alias("ticket_promedio")
    )
    
    resultado.show()
    
    ruta_destino = os.path.join(ZONA_CURATED, "reporte_metodo_pago.parquet")
    resultado.toPandas().to_parquet(ruta_destino, compression="snappy", index=False)
    print(f"✔️ Resultado Job 1 guardado en: {ruta_destino}")

def job2_segmentacion_clientes(spark, ruta_origen):
    print("\n--- Ejecutando Job 2: Segmentación de Clientes ---")
    df = spark.read.parquet(ruta_origen)
    
    if "cliente_id" not in df.columns:
         print("⚠️ Advertencia: La columna 'cliente_id' no existe. Saltando Job 2.")
         return

    # CORRECCIÓN AQUÍ: Cambiamos 'total_venta' por 'total'
    df_clientes = df.groupBy("cliente_id").agg(
        F.count("*").alias("total_compras"),
        F.sum("total").alias("gasto_total")
    )

    resultado = df_clientes.withColumn(
        "segmento",
        F.when(F.col("gasto_total") > 50000000, "VIP")
         .when(F.col("gasto_total") > 10000000, "Premium")
         .when(F.col("gasto_total") > 1000000, "Frecuente")
         .otherwise("Regular")
    )
    
    resultado.show(5)
    
    ruta_destino = os.path.join(ZONA_CURATED, "reporte_segmentacion_clientes.parquet")
    resultado.toPandas().to_parquet(ruta_destino, compression="snappy", index=False)
    print(f"✔️ Resultado Job 2 guardado en: {ruta_destino}")

def job3_analisis_temporal(spark, ruta_origen):
    print("\n--- Ejecutando Job 3: Análisis Temporal (Ventas por Mes) ---")
    df = spark.read.parquet(ruta_origen)
    
    if "anio" in df.columns and "mes" in df.columns:
        # CORRECCIÓN AQUÍ: Cambiamos 'total_venta' por 'total'
        resultado = df.groupBy("anio", "mes").agg(
            F.sum("total").alias("ingresos_mensuales"),
            F.count("*").alias("cantidad_transacciones")
        ).orderBy("anio", "mes")
    # CORRECCIÓN AQUÍ: Cambiamos 'fecha' por 'fecha_venta'
    elif "fecha_venta" in df.columns:
         df_fecha = df.withColumn("fecha_valida", F.to_date(F.col("fecha_venta")))
         df_fecha = df_fecha.withColumn("anio", F.year("fecha_valida")) \
                            .withColumn("mes", F.month("fecha_valida"))
         
         resultado = df_fecha.groupBy("anio", "mes").agg(
            F.sum("total").alias("ingresos_mensuales"),
            F.count("*").alias("cantidad_transacciones")
        ).orderBy("anio", "mes")
    else:
        print("⚠️ Advertencia: No hay columnas de fecha para analizar. Saltando Job 3.")
        return
        
    resultado.show()
    
    ruta_destino = os.path.join(ZONA_CURATED, "reporte_tendencia_mensual.parquet")
    resultado.toPandas().to_parquet(ruta_destino, compression="snappy", index=False)
    print(f"✔️ Resultado Job 3 guardado en: {ruta_destino}")

# ==========================================
# Función Principal
# ==========================================
def ejecutar_jobs_spark():
    try:
        ruta_processed = obtener_archivo_parquet(ZONA_PROCESSED)
        print(f"📄 Archivo origen detectado: {ruta_processed}")

        print("🔥 Iniciando Apache Spark...")
        spark = SparkSession.builder \
            .appName("DataLake_Procesamiento_Fase2") \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")

        job1_ventas_metodo_pago(spark, ruta_processed)
        job2_segmentacion_clientes(spark, ruta_processed)
        job3_analisis_temporal(spark, ruta_processed)

        spark.stop()
        print("\n✅ Todos los Spark Jobs finalizaron exitosamente.")
        
    except Exception as e:
        print(f"\n❌ Ocurrió un error durante la ejecución: {e}")

if __name__ == "__main__":
    ejecutar_jobs_spark()