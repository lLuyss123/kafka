# 📜 Carpeta `spark/jobs/`: los programas de Spark

> **En una frase:** aquí están los **dos programas** que transforman los datos: uno trabaja **en tiempo real** (crudo → limpio) y otro **por lotes** (limpio → resumen de negocio).

⬅️ [Volver a `spark/`](../README.md) · [Volver al README principal](../../README.md)

---

## Vista rápida: cómo cambian los datos

Antes de leer código, mira **qué le pasa a los datos**. Es lo más importante:

```
 KAFKA (texto JSON, tal cual lo envió la app)
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ {"pedido_id":"5b86…","cliente":"cliente_043","ciudad":"Cartagena",…,"total":48000} │
 │ {"pedido_id":"9a1f…","cliente":null,"ciudad":"Cali",…,"total":76000}        ← sin cliente │
 │ {"pedido_id":"c33d…","cliente":"cliente_007","ciudad":"Bogotá",…,"total":-50000} ← negativo │
 └──────────────────────────────────────────────────────────────────────────────┘
          │  streaming_pedidos.py (cada 10 segundos)
          ├─────────────────────────────────────────────┐
          ▼                                             ▼
 🥉 BRONZE: se guarda TODO tal cual            🥈 SILVER: separado en columnas y SOLO lo válido
 ┌──────────────────┬─────────┬────────┐      ┌───────────┬───────────┬──────────┬───────┬────────────┐
 │ json (texto)     │partition│ offset │      │ cliente   │ ciudad    │ producto │ total │ fecha      │
 ├──────────────────┼─────────┼────────┤      ├───────────┼───────────┼──────────┼───────┼────────────┤
 │ {"pedido_id":…}  │    2    │   0    │      │cliente_043│ Cartagena │Empanadas │ 48000 │ 2026-10-14 │
 │ {"pedido_id":…}  │    1    │   40   │      └───────────┴───────────┴──────────┴───────┴────────────┘
 │ {"pedido_id":…}  │    0    │   12   │        (los 2 pedidos con errores NO pasan)
 └──────────────────┴─────────┴────────┘
                                                        │  gold_ventas.py (cuando Airflow lo ordena)
                                                        ▼
                                  🥇 GOLD: resumen agrupado por ciudad
                                  ┌────────────┬──────────────┬─────────────┬────────────────┐
                                  │ fecha      │ ciudad       │ num_pedidos │ ventas_totales │
                                  ├────────────┼──────────────┼─────────────┼────────────────┤
                                  │ 2026-10-14 │ Cartagena    │     106     │    7.524.000   │
                                  │ 2026-10-14 │ Bogotá       │     101     │    6.476.000   │
                                  └────────────┴──────────────┴─────────────┴────────────────┘
```

---

## 📄 Programa 1: `streaming_pedidos.py` (TIEMPO REAL)

**Dónde corre:** en el contenedor `spark-streaming`, que está **siempre encendido**.
**Qué hace:** cada 10 segundos lee lo nuevo de Kafka y lo guarda en bronze y silver.

### Paso 1: encender Spark

```python
spark = (SparkSession.builder.appName("streaming-pedidos")
         .config("spark.sql.shuffle.partitions", "2")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
```

- `SparkSession` es **"la llave de encendido"** de Spark. Todo empieza aquí.
- `appName` es el nombre del trabajo (sale en los logs).
- `shuffle.partitions = 2`: Spark divide el trabajo en pedazos. Por defecto usa 200, demasiados para tan pocos datos, así que lo bajamos a 2.
- `setLogLevel("WARN")`: "solo muéstrame advertencias y errores", para no llenar la pantalla de mensajes.

### Paso 2: decirle a Spark cómo es un pedido (el "esquema")

```python
esquema = (StructType()
           .add("pedido_id", StringType())     # texto
           .add("cliente", StringType())       # texto
           .add("cantidad", IntegerType())     # número entero
           .add("total", LongType())           # número entero grande
           ...)
```

Kafka entrega **texto plano**. El esquema es como **los encabezados de una hoja de Excel**: le dice a Spark qué columnas esperar y de qué tipo es cada una (texto, número…).

### Paso 3: conectarse a Kafka

```python
crudo = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", "kafka:9092")   # dónde está Kafka
         .option("subscribe", "pedidos")                     # qué topic leer
         .option("startingOffsets", "earliest")              # la 1ª vez: desde el principio
         .load()
         .select(F.col("value").cast("string").alias("json"),
                 "partition", "offset",
                 F.col("timestamp").alias("kafka_timestamp")))
```

- **`readStream`** (no `read`): significa "esto es un **flujo continuo**, sigue leyendo para siempre".
- **`startingOffsets: earliest`**: la **primera vez** empieza desde el mensaje más antiguo. Después **ignora esta opción** y continúa desde su marcapáginas (el *checkpoint*, ver Paso 6).
- Kafka entrega cada mensaje con varios datos. Nos quedamos con:
  - `value`: el contenido (el JSON del pedido), convertido a texto;
  - `partition` y `offset`: en qué carril y posición estaba;
  - `timestamp`: cuándo lo recibió Kafka.

### Paso 4: qué hacer con cada micro-lote

Cada 10 segundos, Spark junta los mensajes nuevos en un **micro-lote** (un pequeño grupo) y llama a esta función:

```python
def procesar_microlote(df, batch_id):
    if df.isEmpty():
        return                      # si no llegó nada, no hace nada
    df.persist()                    # "guárdalo en memoria, lo voy a usar varias veces"
```

> 📝 `df` significa **DataFrame**: una **tabla** con filas y columnas, como una hoja de Excel. Es la forma en que Spark representa los datos.

**4a) 🥉 BRONZE: guardar todo tal cual**
```python
    df.write.mode("append").parquet(f"{LAKE}/bronze/pedidos")
```
- `mode("append")`: **añade** al final, sin borrar lo anterior.
- `.parquet(...)`: guarda en formato Parquet (un Excel comprimido y rápido).
- **¿Por qué guardar los datos sucios?** Porque si mañana descubres un error en la limpieza, puedes **reprocesar desde bronze**. Es tu copia de seguridad fiel.

**4b) Convertir el texto JSON en columnas**
```python
    parseado = (df.select(F.from_json("json", esquema).alias("p"), "partition", "offset")
                  .select("p.*", "partition", "offset")
                  .withColumn("fecha_evento", F.to_timestamp("fecha_evento"))
                  .withColumn("fecha", F.to_date("fecha_evento")))
```
- `from_json(...)`: usa el esquema para "abrir" el texto y separarlo en columnas.
- `to_timestamp`: convierte el texto `"2026-10-14T14:59:29..."` en una **fecha y hora de verdad** (con la que se puede calcular).
- `to_date`: crea una columna `fecha` solo con el día (`2026-10-14`).

**4c) 🧹 Limpiar: la regla de calidad**
```python
    validos = (parseado.filter(F.col("cliente").isNotNull() & (F.col("total") > 0))
                       .dropDuplicates(["pedido_id"]))
```
Esta es **la limpieza**. Un pedido es válido si:
- ✅ **tiene cliente** (`isNotNull` = "no está vacío"), **Y**
- ✅ **el total es mayor que 0**.

Después, `dropDuplicates`: si el mismo `pedido_id` aparece dos veces, se queda con uno solo.

**4d) 🥈 SILVER: guardar solo lo limpio**
```python
    validos.write.mode("append").partitionBy("fecha").parquet(f"{LAKE}/silver/pedidos")
```
- `partitionBy("fecha")`: crea **una subcarpeta por día** (`fecha=2026-10-14/`). Así, si mañana alguien pide "solo los pedidos del 14 de octubre", Spark abre una carpeta en vez de revisarlo todo. Es como tener un archivador **con una gaveta por día**.

**4e) Imprimir el resumen**
```python
    print(f"[Micro-lote {batch_id}] leidos de Kafka: {total} (offsets ...) | "
          f"bronze: +{total} | silver: +{buenos} | descartados (sucios): {total - buenos}")
```

### Paso 5: cómo leer la línea que imprime

```
[Micro-lote 7] leidos de Kafka: 20 (offsets 85-150) | bronze: +20 | silver: +19 | descartados (sucios): 1
```

| Parte | Significado |
|---|---|
| `Micro-lote 7` | Es la 8ª ronda (se empieza a contar en 0) |
| `leidos de Kafka: 20` | Llegaron 20 pedidos en estos 10 segundos (2 por segundo × 10) |
| `offsets 85-150` | El offset más bajo y el más alto entre las 3 particiones (cada partición tiene su propia numeración, por eso el rango es amplio) |
| `bronze: +20` | Guardó los 20 tal cual |
| `silver: +19` | 19 pasaron la limpieza |
| `descartados (sucios): 1` | 1 tenía errores (sin cliente o total negativo) |

> 🧠 **Si un micro-lote lee muchos más de lo normal** (por ejemplo 358 o 1496), es porque Spark estuvo apagado y **Kafka guardó los pedidos esperándolo**. ¡No se perdió nada!

### Paso 6: arrancar el flujo, con marcapáginas y reloj

```python
(crudo.writeStream
      .foreachBatch(procesar_microlote)                                   # usa la función del Paso 4
      .option("checkpointLocation", f"{LAKE}/_checkpoints/streaming_pedidos")  # 🔖 marcapáginas
      .trigger(processingTime="10 seconds")                               # ⏰ cada 10 segundos
      .start()
      .awaitTermination())                                                # y no termines nunca
```

- **`checkpointLocation`**: aquí Spark anota **hasta qué offset ha procesado** en cada partición. Si lo apagas y lo vuelves a encender, lee esa nota y **continúa exactamente donde iba**: ni repite ni se salta pedidos. Es el **marcapáginas** de Spark.
- **`trigger 10 seconds`**: el "reloj" de los micro-lotes.
- **`awaitTermination()`**: "quédate funcionando para siempre".

---

## 📄 Programa 2: `gold_ventas.py` (POR LOTES)

**Dónde corre:** dentro del contenedor `airflow`, cuando Airflow lo ordena (cada 10 minutos) o cuando lo ejecutas a mano.
**Qué hace:** lee **todos** los pedidos limpios (silver) y calcula **resúmenes de negocio** (gold). Luego se apaga.

### Paso 1: leer silver

```python
silver = spark.read.parquet(f"{LAKE}/silver/pedidos")
```
Ahora es **`read`** (no `readStream`): lee lo que hay **en este momento** y ya está.

### Paso 2: resumen de ventas por ciudad

```python
ventas = (silver.groupBy("fecha", "ciudad")
                .agg(F.countDistinct("pedido_id").alias("num_pedidos"),
                     F.sum("total").alias("ventas_totales"),
                     F.round(F.avg("total"), 0).alias("ticket_promedio"),
                     F.countDistinct("cliente").alias("clientes_unicos"))
                .orderBy("fecha", F.desc("ventas_totales")))
```

Es exactamente lo que harías con una **tabla dinámica de Excel**:
- `groupBy("fecha", "ciudad")`: "agrupa los pedidos por día y ciudad";
- `countDistinct("pedido_id")`: cuántos pedidos **distintos** hay;
- `sum("total")`: suma del dinero;
- `avg("total")`: el promedio por pedido (el "ticket promedio");
- `countDistinct("cliente")`: cuántos clientes diferentes;
- `orderBy(... desc ...)`: ordena de mayor a menor venta.

### Paso 3: productos más vendidos
```python
top_productos = silver.groupBy("fecha", "producto").agg(sum(cantidad), sum(total))...
```
Lo mismo, pero agrupando por **producto**.

### Paso 4: guardar en gold
```python
ventas.write.mode("overwrite").parquet(f"{LAKE}/gold/ventas_por_ciudad")
top_productos.write.mode("overwrite").parquet(f"{LAKE}/gold/top_productos")
ventas.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{LAKE}/gold/ventas_por_ciudad_csv")
```
- `mode("overwrite")`: **reemplaza** el resultado anterior (gold siempre muestra la foto más reciente).
- `coalesce(1)`: "júntalo todo en **un solo archivo**" (Spark normalmente crea varios, uno por pedazo de trabajo).
- `.csv(...)` con `header`: también lo guarda como **CSV con encabezados**, que se abre con Excel.

### Paso 5: mostrar el resultado en pantalla
```python
ventas.show(truncate=False)
```
Imprime la tabla en el log. En Airflow lo ves en la tarea `spark_calcular_gold` → **Logs**.

---

## Streaming vs batch, lado a lado

| | `streaming_pedidos.py` | `gold_ventas.py` |
|---|---|---|
| Lee de | Kafka | datalake/silver |
| Escribe en | bronze + silver | gold |
| Instrucción clave | `readStream` / `writeStream` | `read` / `write` |
| Cuándo corre | **Siempre**, cada 10 s | Cuando **Airflow** lo ordena |
| Modo de escritura | `append` (añadir) | `overwrite` (reemplazar) |
| Recuerda por dónde iba | Sí (checkpoint) | No lo necesita (siempre lee todo) |
| Contenedor | `spark-streaming` | `airflow` |

---

## Comandos útiles

```bash
cd ~/pipeline-datos

# Ver los micro-lotes en vivo
docker compose logs -f spark-streaming | grep Micro

# Ejecutar gold a mano
docker exec airflow spark-submit --master 'local[2]' --driver-memory 512m /opt/jobs/gold_ventas.py

# Ver silver como una tabla (usa la caja de Airflow, que tiene pandas)
docker exec airflow python -c "import pandas as pd; df=pd.read_parquet('/opt/datalake/silver/pedidos'); print(len(df), 'pedidos limpios'); print(df.head(10).to_string(index=False))"

# Si modificas streaming_pedidos.py, reinicia para que tome el cambio
docker compose restart spark-streaming
```

## 🧪 Ejercicio: cambia la regla de limpieza

1. En `streaming_pedidos.py`, cambia la regla para que también descarte los pedidos de más de 4 unidades:
   ```python
   validos = (parseado.filter(F.col("cliente").isNotNull() & (F.col("total") > 0) & (F.col("cantidad") <= 4))
   ```
2. Ejecuta `docker compose restart spark-streaming`.
3. Mira los micro-lotes. Como el productor nunca genera más de 4 unidades, **nada cambia**. Prueba ahora con `<= 2` y verás cómo sube la columna "descartados".

➡️ **Siguiente parada:** [`airflow/`](../../airflow/README.md), quien decide cuándo se ejecuta `gold_ventas.py`.
