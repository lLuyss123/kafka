"""
SPARK BATCH: silver -> gold
Lo ejecuta Airflow según su horario. Toma TODOS los pedidos limpios
y calcula las métricas de negocio que verá el dashboard.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LAKE = "/opt/datalake"

spark = SparkSession.builder.appName("gold-ventas").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

silver = spark.read.parquet(f"{LAKE}/silver/pedidos")

ventas = (silver.groupBy("fecha", "ciudad")
                .agg(F.countDistinct("pedido_id").alias("num_pedidos"),
                     F.sum("total").alias("ventas_totales"),
                     F.round(F.avg("total"), 0).alias("ticket_promedio"),
                     F.countDistinct("cliente").alias("clientes_unicos"))
                .orderBy("fecha", F.desc("ventas_totales")))

top_productos = (silver.groupBy("fecha", "producto")
                       .agg(F.sum("cantidad").alias("unidades"),
                            F.sum("total").alias("ventas_totales"))
                       .orderBy("fecha", F.desc("ventas_totales")))

ventas.write.mode("overwrite").parquet(f"{LAKE}/gold/ventas_por_ciudad")
top_productos.write.mode("overwrite").parquet(f"{LAKE}/gold/top_productos")
# También en CSV (1 solo archivo) para abrirlo fácil en Excel
ventas.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{LAKE}/gold/ventas_por_ciudad_csv")

print("=== GOLD: ventas por ciudad ===")
ventas.show(truncate=False)
print("=== GOLD: top productos ===")
top_productos.show(truncate=False)
spark.stop()
