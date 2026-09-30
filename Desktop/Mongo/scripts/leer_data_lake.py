import os
import pandas as pd

# 1. Definir rutas
DATA_LAKE_ROOT = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data_lake")
ruta_curated = os.path.join(DATA_LAKE_ROOT, "curated", "fact_ventas.parquet")
ruta_processed = os.path.join(DATA_LAKE_ROOT, "processed", "ventas_procesadas.parquet")

def mostrar_resultados():
    print("\n" + "="*50)
    print("🔍 LECTURA DEL DATA LAKE - ZONA CURATED (Analítica)")
    print("="*50)

    try:
        # Leer el archivo Parquet
        df_fact = pd.read_parquet(ruta_curated)
        
        # Mostrar información técnica (A los profesores les encanta esto)
        print("\n📊 Información de la tabla (Tipos de datos y memoria):")
        df_fact.info()

        print("\n👀 Primeros 5 registros de la tabla de hechos:")
        # Mostrar todas las columnas sin que Pandas las recorte
        pd.set_option('display.max_columns', None) 
        print(df_fact.head())

        # Mostrar una pequeña estadística
        print("\n📈 Estadísticas rápidas:")
        print(f"Total de ventas procesadas: {len(df_fact)}")
        
        # Validar si extrajo bien el año y mes simulando Spark
        if 'anio' in df_fact.columns and 'mes' in df_fact.columns:
            print("\nAgrupación por Año y Mes (Prueba de transformación):")
            print(df_fact.groupby(['anio', 'mes']).size().reset_index(name='cantidad_ventas'))

    except FileNotFoundError:
        print("❌ Error: No se encontró el archivo Parquet. Ejecuta el pipeline primero.")

if __name__ == "__main__":
    mostrar_resultados()