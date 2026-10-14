# 🗄️ Carpeta `datalake/`: el ALMACÉN donde terminan los datos

> **En una frase:** aquí se **guardan** los pedidos en 3 niveles de calidad (🥉 crudo → 🥈 limpio → 🥇 resumido) y los **reportes finales**. Esta carpeta empieza **vacía** y se llena **sola** cuando el pipeline funciona.

⬅️ [Volver al README principal](../README.md)

---

## 1. ¿Qué es un Data Lake?

Un **data lake** ("lago de datos") es un **gran almacén de archivos** donde una empresa guarda **todos** sus datos: crudos, limpios, resumidos, en el formato que sea.

- En una empresa real, el data lake está en la nube: **Amazon S3**, **Azure Data Lake** o **Google Cloud Storage**, con terabytes de datos.
- **Aquí es simplemente esta carpeta.** El concepto es el mismo; solo cambia el tamaño.

### La "arquitectura medallón" 🥉🥈🥇

Los datos se organizan en **3 niveles**, como las medallas olímpicas. Cada nivel es **más limpio y más útil** que el anterior:

```
   🥉 BRONZE              🥈 SILVER               🥇 GOLD
   "La materia prima"     "Lo lavado y cortado"   "El plato servido"
   ───────────────        ─────────────────       ───────────────────
   Tal cual llegó         Columnas ordenadas      Resúmenes listos
   Con errores            Sin errores             para el jefe
   Todo                   Sin duplicados          (ventas por ciudad)
        │                       │                        │
   ¿Quién lo usa?          ¿Quién lo usa?           ¿Quién lo usa?
   Ingenieros, para        Analistas y              Gerentes, dashboards,
   reprocesar si algo      científicos de datos     reportes
   sale mal
```

> 🍳 **Analogía de cocina:** **bronze** son los ingredientes como llegaron del mercado (con tierra, alguno podrido). **Silver** son los ingredientes lavados, pelados y cortados. **Gold** es el plato terminado, listo para servir.

---

## 2. ¿Quién escribe aquí y quién lee?

```
                         ┌──────────────── datalake/ ────────────────────┐
                         │                                               │
 ⚙️ Spark Streaming ───► │ 🥉 bronze/pedidos/         (escribe cada 10 s) │
 (cada 10 segundos)  ──► │ 🥈 silver/pedidos/         (escribe cada 10 s) │
                    ───► │ 🔖 _checkpoints/           (su marcapáginas)   │
                         │          │                                    │
                         │          │ lee                                │
                         │          ▼                                    │
 ⚙️ Spark Batch ───────► │ 🥇 gold/                   (reemplaza)        │
 (lo lanza Airflow)      │          │                                    │
                         │          │ lee y copia                        │
 🟩 Airflow ───────────► │ 📄 reportes/               (añade uno nuevo)  │
 (cada 10 minutos)       │                                               │
                         └───────────────────────────────────────────────┘
                                    ▲
                                    │  ¡Y TÚ también la ves desde tu PC!
```

### ¿Por qué la ven a la vez tu PC, Spark y Airflow?

Porque en `docker-compose.yml` esta carpeta está **compartida** con las cajas:

```yaml
volumes:
  - ./datalake:/opt/datalake      # tu carpeta datalake  =  /opt/datalake dentro de la caja
```

Es **una sola carpeta vista desde varios lugares**:

| Desde | Se llama |
|---|---|
| Tu PC | `~/pipeline-datos/datalake/` |
| Contenedor `spark-streaming` | `/opt/datalake/` |
| Contenedor `airflow` | `/opt/datalake/` |

Por eso, cuando Spark escribe un archivo dentro de su caja, **aparece en tu explorador de archivos**.

---

## 3. ¿Qué hay en cada subcarpeta?

Después de unos minutos funcionando, la carpeta se ve así:

```
datalake/
├── README.md                        ← este archivo
│
├── bronze/
│   └── pedidos/                     🥉 TODOS los mensajes de Kafka, sin tocar
│       ├── part-00000-…snappy.parquet     (un archivo nuevo por cada micro-lote)
│       ├── part-00001-…snappy.parquet
│       └── _SUCCESS
│
├── silver/
│   └── pedidos/                     🥈 Pedidos limpios, organizados por día
│       ├── fecha=2026-10-14/              ← una "gaveta" por día
│       │   ├── part-00000-…snappy.parquet
│       │   └── …
│       ├── fecha=2026-10-15/              (aparecerá mañana)
│       └── _SUCCESS
│
├── gold/                            🥇 Resúmenes (se REEMPLAZAN en cada ejecución)
│   ├── ventas_por_ciudad/                 ventas por día y ciudad (parquet)
│   ├── top_productos/                     ventas por día y producto (parquet)
│   └── ventas_por_ciudad_csv/             lo mismo que ventas_por_ciudad, en CSV
│       └── part-00000-…csv
│
├── reportes/                        📄 El resultado final, ¡ábrelo con Excel!
│   ├── ventas_20261014T145000.csv
│   ├── ventas_20261014T150000.csv         (uno nuevo cada 10 minutos)
│   └── …
│
└── _checkpoints/                    🔖 El marcapáginas de Spark Streaming (¡NO TOCAR!)
    └── streaming_pedidos/
        ├── offsets/                       qué offsets va a procesar en cada micro-lote
        ├── commits/                       qué micro-lotes terminó
        ├── metadata
        └── sources/
```

### Detalle de cada una

#### 🥉 `bronze/pedidos/`
- **Contenido:** cada mensaje de Kafka **exactamente como llegó** (el texto JSON), más `partition`, `offset` y `kafka_timestamp`.
- **Incluye los datos sucios** (sin cliente o con el total negativo).
- **Modo:** se **añade** (nunca se borra nada).
- **¿Para qué?** Es la **copia de seguridad fiel**. Si mañana descubres un error en la limpieza, puedes reprocesar desde aquí sin depender de Kafka (que borra los mensajes después de 7 días).

#### 🥈 `silver/pedidos/`
- **Contenido:** pedidos **separados en columnas** (`pedido_id`, `cliente`, `ciudad`, `producto`, `cantidad`, `precio_unitario`, `total`, `fecha_evento`, `partition`, `offset`), **sin errores** y **sin duplicados**.
- **Las carpetas `fecha=2026-10-14`** son **particiones por día**. Si alguien pide "las ventas del 14 de octubre", Spark abre solo esa carpeta en vez de revisar todo. Es como un archivador con **una gaveta por día**.
- **Modo:** se **añade** cada 10 segundos.
- **¿Para qué?** Es **la fuente confiable** para cualquier análisis.

#### 🥇 `gold/`
- **Contenido:** **resúmenes** calculados a partir de silver:
  - `ventas_por_ciudad/`: `fecha`, `ciudad`, `num_pedidos`, `ventas_totales`, `ticket_promedio`, `clientes_unicos`;
  - `top_productos/`: `fecha`, `producto`, `unidades`, `ventas_totales`;
  - `ventas_por_ciudad_csv/`: lo mismo que `ventas_por_ciudad`, pero en CSV.
- **Modo:** se **reemplaza** en cada ejecución: siempre muestra la "foto" más reciente.
- **¿Para qué?** Es lo que consumen los **dashboards** (Power BI, Looker, Metabase…) y los reportes.

#### 📄 `reportes/`
- **Contenido:** una copia del CSV de gold **con fecha y hora en el nombre**: `ventas_20261014T150337.csv` = 14 de octubre de 2026, 15:03:37 (hora UTC).
- **Modo:** se **añade uno nuevo** en cada ejecución de Airflow, así queda un **historial**.
- **¿Para qué?** Es el **entregable final** para el "jefe". **Ábrelo con doble clic en Excel / LibreOffice.**

#### 🔖 `_checkpoints/`
- **Contenido:** archivos internos de Spark donde anota **hasta qué offset procesó** en cada partición de Kafka. Por ejemplo, uno de sus archivos dice:
  ```json
  {"pedidos":{"2":356,"1":434,"0":550}}
  ```
  Es decir: *"del topic pedidos, en la partición 2 voy por el 356, en la 1 por el 434 y en la 0 por el 550"*.
- **¿Para qué?** Si Spark se apaga, al volver lee esto y **continúa exactamente donde iba**.
- ⚠️ **No lo borres** salvo que quieras que Spark **vuelva a leer todo Kafka desde el principio**.

---

## 4. Archivos raros que vas a ver (y qué significan)

| Archivo | ¿Qué es? |
|---|---|
| `part-00000-02e3…c000.snappy.parquet` | Un pedazo de datos. Spark divide los datos en "partes" (`part`) y cada una tiene un código único. `snappy` es el tipo de compresión |
| `_SUCCESS` | Un archivo **vacío** que Spark crea al terminar de escribir. Significa "esta carpeta está completa y lista para leer" |
| `.part-….crc`, `._SUCCESS.crc` | Archivos ocultos de "suma de verificación": sirven para detectar si un archivo se dañó |

> 📝 **¿Por qué hay tantos archivos `part-…`?** Porque cada micro-lote (cada 10 segundos) escribe archivos nuevos. Tras 1 hora tendrás cientos. En una empresa se ejecuta un proceso de **compactación** que los junta. Aquí no importa.

---

## 5. ¿Qué es Parquet y por qué no usamos CSV para todo?

| | CSV | Parquet |
|---|---|---|
| ¿Se abre con Excel? | ✅ Sí | ❌ No directamente |
| Tamaño | Grande | **Hasta 10 veces más pequeño** (comprimido) |
| Velocidad para leer una columna | Lento: lee el archivo completo | **Rápido**: lee solo esa columna |
| Guarda el tipo de dato (número, fecha…) | ❌ Todo es texto | ✅ Sí |
| Uso típico | Compartir con personas | Guardar y procesar grandes datos |

Por eso se usa **Parquet para guardar** (bronze, silver, gold) y **CSV solo para el reporte final** que lee una persona.

---

## 6. Cómo VER los datos

Ejecútalos desde la carpeta del proyecto (`cd ~/pipeline-datos`). Los comandos que usan `docker exec airflow` necesitan que **Airflow esté encendido** (lo usamos porque tiene pandas instalado).

```bash
# Ver la estructura de carpetas
ls -R datalake | head -40

# ¿Cuánto ocupa cada nivel?
du -sh datalake/bronze datalake/silver datalake/gold

# 🥉 Ver bronze (los mensajes crudos)
docker exec airflow python -c "import pandas as pd; df=pd.read_parquet('/opt/datalake/bronze/pedidos'); print(len(df), 'mensajes en bronze'); print(df[['partition','offset','json']].head(5).to_string(index=False))"

# 🥈 Ver silver (los pedidos limpios)
docker exec airflow python -c "import pandas as pd; df=pd.read_parquet('/opt/datalake/silver/pedidos'); print(len(df), 'pedidos limpios'); print(df[['cliente','ciudad','producto','cantidad','total']].head(10).to_string(index=False))"

# 🥇 Ver gold (productos más vendidos)
docker exec airflow python -c "import pandas as pd; print(pd.read_parquet('/opt/datalake/gold/top_productos').to_string(index=False))"

# 📄 Ver el último reporte como tabla
column -s, -t < "$(ls datalake/reportes/*.csv | tail -1)"
```

**🧪 Pregunta para pensar:** compara cuántos mensajes hay en bronze y cuántos pedidos en silver. ¿Por qué silver tiene **menos**? *(Respuesta: silver no incluye los ~4% de pedidos sucios.)*

---

## 7. Cómo VACIAR el almacén (empezar de cero)

```bash
cd ~/pipeline-datos
docker compose down                      # primero apaga todo
rm -rf datalake/bronze datalake/silver datalake/gold datalake/reportes datalake/_checkpoints
```

> ⚠️ **No uses `rm -rf datalake/*`**: borraría también este README.
>
> ⚠️ **Si apagas con `docker compose down`, vacía también `_checkpoints/`.** Kafka pierde sus mensajes al apagarse con `down`, y el marcapáginas de Spark apuntaría a mensajes que ya no existen.

---

## 8. Resumen

- ✅ El **data lake** es el almacén. Aquí es esta carpeta; en una empresa sería S3, Azure o GCS.
- ✅ **🥉 bronze** = crudo (todo) · **🥈 silver** = limpio (por día) · **🥇 gold** = resúmenes · **📄 reportes** = CSV final.
- ✅ **Spark Streaming** escribe bronze y silver; **Spark Batch** escribe gold; **Airflow** escribe los reportes.
- ✅ Es una **carpeta compartida**: tú, Spark y Airflow ven los mismos archivos.
- ✅ **Parquet** para guardar (pequeño y rápido), **CSV** para personas.
- ✅ **`_checkpoints/`** es el marcapáginas de Spark: no lo toques sin saber lo que haces.
- 🏁 **¡Fin del recorrido!** Vuelve al [README principal](../README.md) para ver el panorama completo.
