"""
SPARK STRUCTURED STREAMING: Kafka -> Data Lake (bronze y silver)

Cada 10 segundos Spark:
  1. Lee de Kafka los pedidos nuevos (desde su último offset; lo recuerda en el checkpoint).
  2. BRONZE: guarda los mensajes tal cual llegaron (copia fiel, por si hay que reprocesar).
  3. SILVER: los convierte en columnas, descarta los sucios y elimina duplicados.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructType

LAKE = "/opt/datalake"

spark = (SparkSession.builder.appName("streaming-pedidos")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

esquema = (StructType()
           .add("pedido_id", StringType())
           .add("cliente", StringType())
           .add("ciudad", StringType())
           .add("producto", StringType())
           .add("cantidad", IntegerType())
           .add("precio_unitario", LongType())
           .add("total", LongType())
           .add("fecha_evento", StringType()))

# 1) Fuente: el topic "pedidos" de Kafka
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
         .option("subscribe", "pedidos")
         .option("startingOffsets", "earliest")
         .load()
         .select(F.col("value").cast("string").alias("json"),
                 "partition", "offset",
                 F.col("timestamp").alias("kafka_timestamp")))


def procesar_microlote(df, batch_id):
    if df.isEmpty():
        return
    df.persist()

    # 2) BRONZE: tal cual llegó
    df.write.mode("append").parquet(f"{LAKE}/bronze/pedidos")

    # 3) SILVER: estructurado y limpio
    parseado = (df.select(F.from_json("json", esquema).alias("p"), "partition", "offset")
                  .select("p.*", "partition", "offset")
                  .withColumn("fecha_evento", F.to_timestamp("fecha_evento"))
                  .withColumn("fecha", F.to_date("fecha_evento")))
    validos = (parseado.filter(F.col("cliente").isNotNull() & (F.col("total") > 0))
                       .dropDuplicates(["pedido_id"]))
    validos.write.mode("append").partitionBy("fecha").parquet(f"{LAKE}/silver/pedidos")

    total, buenos = df.count(), validos.count()
    rango = df.agg(F.min("offset"), F.max("offset")).first()
    print(f"[Micro-lote {batch_id}] leidos de Kafka: {total} (offsets {rango[0]}-{rango[1]}) | "
          f"bronze: +{total} | silver: +{buenos} | descartados (sucios): {total - buenos}",
          flush=True)
    df.unpersist()


(crudo.writeStream
      .foreachBatch(procesar_microlote)
      .option("checkpointLocation", f"{LAKE}/_checkpoints/streaming_pedidos")
      .trigger(processingTime="10 seconds")
      .start()
      .awaitTermination())
