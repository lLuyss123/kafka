# ⚙️ Carpeta `spark/`: la máquina que LIMPIA y RESUME los datos

> **En una frase:** aquí está todo lo necesario para que **Spark** tome los pedidos de Kafka, los limpie, los guarde en el almacén (`datalake/`) y calcule resúmenes de ventas.

⬅️ [Volver al README principal](../README.md)

---

## 1. ¿Qué es Spark? (sin tecnicismos)

Imagina que tienes que revisar **un millón de facturas**:

- **Tú solo** tardarías semanas.
- **Con 100 personas** repartiéndose las facturas, acabarías en horas.

**Spark hace exactamente eso con los datos:** divide el trabajo en pedazos y los procesa **en paralelo** (al mismo tiempo), usando varios núcleos del procesador o incluso cientos de computadores.

En este proyecto los datos son pocos, así que Spark usa solo **2 núcleos de tu PC** (`local[2]`). Pero **el código sería idéntico** con miles de millones de pedidos en un clúster de 100 máquinas. Esa es la gracia de aprender Spark.

### Spark trabaja de dos formas, y aquí usamos las dos

| Modo | ¿Cómo trabaja? | Analogía | Archivo |
|---|---|---|---|
| **Streaming** (tiempo real) | Nunca se apaga. Cada **10 segundos** recoge lo nuevo de Kafka y lo procesa | Un cajero que atiende a cada cliente que llega | `jobs/streaming_pedidos.py` |
| **Batch** (por lotes) | Se enciende, procesa **todo** de una vez, entrega el resultado y **se apaga** | El contador que cierra la caja al final del día | `jobs/gold_ventas.py` |

---

## 2. ¿Qué papel cumple en el pipeline?

```
            🟦 KAFKA (topic "pedidos")
                    │
                    ▼
   ┌────────────────────────────────────┐
   │ ⚙️ SPARK STREAMING                   │  ◄── ESTÁS AQUÍ
   │    jobs/streaming_pedidos.py        │      (contenedor "spark-streaming", siempre encendido)
   └───────────────┬────────────────────┘
          ┌────────┴─────────┐
          ▼                  ▼
   🥉 datalake/bronze   🥈 datalake/silver
                             │
          🟩 AIRFLOW lo ordena cada 10 minutos
                             ▼
   ┌────────────────────────────────────┐
   │ ⚙️ SPARK BATCH                       │  ◄── TAMBIÉN AQUÍ
   │    jobs/gold_ventas.py              │      (corre dentro del contenedor "airflow")
   └───────────────┬────────────────────┘
                   ▼
            🥇 datalake/gold
```

---

## 3. ¿Qué archivos hay?

```
spark/
├── Dockerfile        ← la receta de la caja: Airflow + Java + Spark + conector de Kafka
├── README.md         ← este archivo
└── jobs/             ← los PROGRAMAS de Spark (ver jobs/README.md)
    ├── streaming_pedidos.py   ← tiempo real: Kafka → bronze + silver
    └── gold_ventas.py         ← por lotes:   silver → gold
```

> 📝 **"Job"** (trabajo) es como se llama a un programa de Spark.

---

## 4. `Dockerfile`, línea por línea

```dockerfile
FROM apache/airflow:2.10.5-python3.11
```
**Empieza con la caja oficial de Airflow.** ¿Por qué Airflow, si esta carpeta es de Spark? Porque **la misma caja la usan dos contenedores**:
- `spark-streaming`, que solo usa la parte de Spark;
- `airflow`, que necesita Spark porque una de sus tareas ejecuta `gold_ventas.py`.

Hacer una sola caja **ahorra ~1 GB de disco**. En una empresa real estarían separadas (Airflow en un servidor y Spark en un clúster).

```dockerfile
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
 ...
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```
**Instala Java.** Spark está escrito en Scala, que funciona sobre **Java**. Aunque nosotros escribimos en Python, por debajo Spark necesita Java para funcionar. `JAVA_HOME` le dice dónde está instalado. Se necesita `USER root` (administrador) para instalar programas.

```dockerfile
USER airflow
RUN pip install --no-cache-dir "apache-airflow==2.10.5" "pyspark==3.5.1"
```
**Instala PySpark**, que es Spark controlado desde Python. Se repite `apache-airflow==2.10.5` para que pip **no cambie la versión** de Airflow por accidente.

```dockerfile
RUN JARS=$(python -c "...pyspark/jars...") \
 && curl ... spark-sql-kafka-0-10_2.12-3.5.1.jar \
 && curl ... spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
 && curl ... kafka-clients-3.4.1.jar \
 && curl ... commons-pool2-2.11.1.jar
```
**Descarga el "conector" Spark ↔ Kafka.** Spark no sabe hablar con Kafka de fábrica. Estos 4 archivos `.jar` (piezas de Java) son el **adaptador**, como un enchufe de viaje. Se descargan **una sola vez** al construir la caja, y no cada vez que se enciende.

---

## 5. ¿Cómo se ejecuta un job de Spark?

Con el comando **`spark-submit`** ("enviar a Spark"). Mira cómo lo usa `docker-compose.yml`:

```bash
spark-submit --master local[2] --driver-memory 512m --conf spark.ui.enabled=false /opt/jobs/streaming_pedidos.py
```

| Parte | Significado |
|---|---|
| `spark-submit` | "Spark, ejecuta este programa" |
| `--master local[2]` | "Trabaja **en esta misma máquina** con **2 núcleos** en paralelo". En una empresa diría algo como `--master yarn` o `k8s://...` para usar un clúster |
| `--driver-memory 512m` | "Usa como máximo **512 MB de RAM**" (tu Docker tiene poca memoria) |
| `--conf spark.ui.enabled=false` | "No enciendas tu página web interna" (ahorra memoria) |
| `/opt/jobs/streaming_pedidos.py` | El programa a ejecutar |

### ¿Por qué `/opt/jobs/` y no `spark/jobs/`?

Por las **carpetas compartidas** (*volúmenes*) de `docker-compose.yml`:

```yaml
volumes:
  - ./spark/jobs:/opt/jobs        # tu carpeta spark/jobs  =  /opt/jobs dentro de la caja
  - ./datalake:/opt/datalake      # tu carpeta datalake    =  /opt/datalake dentro de la caja
```

Es **la misma carpeta vista desde dos lugares**, como una carpeta compartida de Google Drive:
- Si editas `spark/jobs/gold_ventas.py` en tu PC, la caja ve el cambio **inmediatamente** (no hay que reconstruir nada).
- Cuando Spark escribe en `/opt/datalake/silver`, el archivo aparece en tu `datalake/silver`.

---

## 6. Comandos para ver Spark en acción

Ejecútalos desde la carpeta del proyecto (`cd ~/pipeline-datos`):

```bash
# Encender Spark Streaming (Kafka debe estar encendido)
docker compose up -d spark-streaming

# Ver los micro-lotes en vivo                (Ctrl + C para dejar de mirar)
docker compose logs -f spark-streaming | grep Micro

# Ejecutar el job batch (gold) a mano, sin esperar a Airflow
docker exec airflow spark-submit --master 'local[2]' --driver-memory 512m /opt/jobs/gold_ventas.py

# Ver cuánta memoria usa cada caja
docker stats --no-stream

# Apagar / encender Spark Streaming (para el experimento de "Kafka no pierde nada")
docker compose stop spark-streaming
docker compose start spark-streaming
```

**Qué verás en los micro-lotes:**
```
[Micro-lote 0] leidos de Kafka: 358 (offsets 0-138) | bronze: +358 | silver: +351 | descartados (sucios): 7
[Micro-lote 1] leidos de Kafka: 10 (offsets 82-144) | bronze: +10 | silver: +9 | descartados (sucios): 1
```
La explicación detallada de estas líneas está en [`jobs/README.md`](jobs/README.md).

---

## 7. Detalles técnicos que resolvimos (por si los ves en los logs)

| Problema | Por qué pasa | Cómo se resolvió |
|---|---|---|
| `java.lang.NullPointerException: invalid null input: name` | Spark necesita un **nombre de usuario**, y tu usuario (número 1003) no existe dentro de la caja | Se lanza Spark con `bash -c "..."`: así la caja de Airflow registra tu usuario al arrancar |
| `basedir must be absolute: ?/.ivy2` | Java no encontraba una "carpeta personal" | `SPARK_SUBMIT_OPTS: "-Duser.home=/tmp"` en `docker-compose.yml` |
| `Exited (137)` / `OOMKilled` | **Sin memoria**: Docker Desktop tiene ~3,7 GB y dos Spark + Airflow no cabían | `--driver-memory 512m`, límites de memoria en Kafka/Airflow y `restart: unless-stopped` |

> 💡 Si tienes RAM de sobra, puedes darle más a Docker Desktop en **Settings → Resources → Memory**.

---

## 8. Resumen

- ✅ **Spark** procesa datos **en paralelo**. Aquí usa 2 núcleos, pero el código escalaría a un clúster.
- ✅ **Streaming** (`streaming_pedidos.py`): siempre encendido, procesa cada 10 s → bronze + silver.
- ✅ **Batch** (`gold_ventas.py`): lo lanza Airflow, procesa todo de una vez → gold.
- ✅ Necesita **Java** y un **conector** para hablar con Kafka (ya incluidos en el `Dockerfile`).
- ✅ Las **carpetas compartidas** hacen que tu PC y la caja vean los mismos archivos.
- ➡️ **Siguiente:** [`jobs/README.md`](jobs/README.md) explica el código paso a paso.
