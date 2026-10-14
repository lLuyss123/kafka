"""
DAG: pipeline_pedidos
Airflow NO mueve los datos: decide QUÉ se ejecuta, CUÁNDO y en qué ORDEN.

    verificar_silver  >>  spark_calcular_gold  >>  validar_calidad  >>  publicar_reporte

Si un paso falla, los siguientes no se ejecutan y Airflow lo marca en rojo.
"""
import glob
import os
import shutil
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

LAKE = "/opt/datalake"


def verificar_silver():
    """Paso 1: ¿Spark Streaming ya dejó datos limpios? Si no, no tiene sentido seguir."""
    archivos = glob.glob(f"{LAKE}/silver/pedidos/**/*.parquet", recursive=True)
    if not archivos:
        raise ValueError("No hay datos en silver todavía. ¿Está corriendo spark-streaming?")
    print(f"OK: {len(archivos)} archivos parquet en silver.")


def validar_calidad():
    """Paso 3: revisar que el resultado tenga sentido antes de publicarlo."""
    df = pd.read_parquet(f"{LAKE}/gold/ventas_por_ciudad")
    print(df.to_string(index=False))
    assert len(df) > 0, "La tabla gold está vacía"
    assert (df["ventas_totales"] > 0).all(), "Hay ventas negativas: la limpieza falló"
    assert df["ciudad"].notna().all(), "Hay ciudades vacías"
    print(f"Calidad OK: {len(df)} filas, ventas totales = ${df['ventas_totales'].sum():,}")


def publicar_reporte(ts_nodash, **_):
    """Paso 4: dejar el reporte listo para negocio (aquí: un CSV con fecha y hora)."""
    origen = glob.glob(f"{LAKE}/gold/ventas_por_ciudad_csv/part-*.csv")[0]
    os.makedirs(f"{LAKE}/reportes", exist_ok=True)
    destino = f"{LAKE}/reportes/ventas_{ts_nodash}.csv"
    shutil.copy(origen, destino)
    print(f"Reporte publicado en {destino}")


with DAG(
    dag_id="pipeline_pedidos",
    description="Silver -> Gold con Spark, validación y reporte",
    start_date=datetime(2026, 1, 1),
    schedule="*/10 * * * *",            # cada 10 minutos (en la vida real: diario, 2 a. m.)
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["aprendizaje", "spark", "kafka"],
) as dag:

    t1 = PythonOperator(task_id="verificar_silver", python_callable=verificar_silver)

    t2 = BashOperator(
        task_id="spark_calcular_gold",
        bash_command="spark-submit --master local[2] --driver-memory 512m --conf spark.ui.enabled=false /opt/jobs/gold_ventas.py",
    )

    t3 = PythonOperator(task_id="validar_calidad", python_callable=validar_calidad)

    t4 = PythonOperator(task_id="publicar_reporte", python_callable=publicar_reporte)

    t1 >> t2 >> t3 >> t4
