# 🍔 Pipeline de datos para principiantes

### Kafka + Spark + Airflow: cómo se une todo, paso por paso

> **¿Para quién es esta guía?** Para alguien que **nunca ha visto** Kafka, Spark ni Airflow. No necesitas saber programar: solo tienes que copiar comandos y leer qué significa lo que aparece.
>
> **Este README es el "mapa general":** explica **cómo se conectan las piezas** y **cómo encenderlo todo**. Cada carpeta tiene su propio `README.md` que explica **en detalle** lo que hay dentro.

---

## 🧭 Cómo leer esta documentación

Te recomiendo este orden: **primero** este README hasta la Parte 4 (para entender la idea), **después** el Paso a paso (Parte 5) y, mientras enciendes cada pieza, la guía de su carpeta:

| Orden | Archivo | Qué aprendes |
|---|---|---|
| 1 | **`README.md`** (este) | La historia, las piezas y **cómo se unen** |
| 2 | [`productor/README.md`](productor/README.md) | Cómo **nacen** los datos y cómo se envían a Kafka |
| 3 | [`spark/README.md`](spark/README.md) | Qué es Spark y cómo está montado |
| 4 | [`spark/jobs/README.md`](spark/jobs/README.md) | El código que **limpia** y **resume** los datos, paso a paso |
| 5 | [`airflow/README.md`](airflow/README.md) | Qué es Airflow y cómo usar su página web |
| 6 | [`airflow/dags/README.md`](airflow/dags/README.md) | La lista de tareas (DAG), tarea por tarea |
| 7 | [`datalake/README.md`](datalake/README.md) | Dónde y cómo se **guardan** los datos (bronze, silver, gold) |

> 💡 **Para leerlos bonitos** (con tablas y colores): ábrelos en **VS Code** y pulsa `Ctrl + Shift + V`. O súbelos a GitHub, donde se ven formateados automáticamente.

---

## 📑 Índice

1. [La historia: ¿qué problema resolvemos?](#parte-1--la-historia-qué-problema-resolvemos)
2. [Las 5 piezas en 1 minuto](#parte-2--las-5-piezas-en-1-minuto)
3. [El mapa completo](#parte-3--el-mapa-completo)
4. [⭐ CÓMO SE UNE TODO](#parte-4--cómo-se-une-todo)
5. [⭐ Paso a paso: encender el pipeline](#parte-5--paso-a-paso-encender-el-pipeline)
6. [Experimentos para entender de verdad](#parte-6--experimentos-para-entender-de-verdad)
7. [Apagar, encender y empezar de cero](#parte-7--apagar-encender-y-empezar-de-cero)
8. [Si algo falla](#parte-8--si-algo-falla)
9. [¿Y RabbitMQ?](#parte-9--y-rabbitmq)
10. [Glosario](#parte-10--glosario)
11. [Chuleta de comandos](#parte-11--chuleta-de-comandos)

---

## Parte 1 — La historia: ¿qué problema resolvemos?

Imagina que trabajas en una **app de domicilios** (como Rappi). Cada segundo, clientes de Barranquilla, Bogotá, Medellín, Cali y Cartagena piden hamburguesas, pizzas, sushi…

El jefe te pide tres cosas:

1. 🚨 **"Quiero enterarme AL INSTANTE de los pedidos grandes."** → necesitamos algo en **tiempo real**.
2. 🧹 **"Quiero guardar TODOS los pedidos, pero limpios: sin datos con errores."** → necesitamos **procesar y almacenar**.
3. 📊 **"Cada cierto tiempo quiero un reporte de ventas por ciudad."** → necesitamos **automatizar tareas programadas**.

Este proyecto resuelve las tres. Como no tenemos clientes reales, **un programa inventa pedidos falsos sin parar**, como si fueran reales.

---

## Parte 2 — Las 5 piezas en 1 minuto

| Pieza | Analogía | Qué hace aquí | Carpeta |
|---|---|---|---|
| 📱 **Productor** | El **periodista** | Inventa 2 pedidos por segundo y los envía a Kafka | [`productor/`](productor/README.md) |
| 🟦 **Kafka** | La **cinta transportadora** (o un periódico con hemeroteca) | Recibe los pedidos, los guarda en orden y deja que **varios** los lean | *(imagen oficial, sin carpeta)* |
| ⚙️ **Spark** | La **máquina procesadora** | Limpia los pedidos (tiempo real) y calcula resúmenes (por lotes) | [`spark/`](spark/README.md) |
| 🟩 **Airflow** | El **jefe de planta** con reloj | Decide **cuándo** y **en qué orden** se ejecutan las tareas | [`airflow/`](airflow/README.md) |
| 🗄️ **Data Lake** | El **almacén** con 3 estantes | Guarda los datos: 🥉 crudos → 🥈 limpios → 🥇 resumidos | [`datalake/`](datalake/README.md) |

Y una más, que lo envuelve todo:

| 🐳 **Docker** | Las **cajas** | Cada pieza vive en una caja (**contenedor**) con todo lo que necesita. No hay que instalar Kafka, Spark ni Airflow en tu PC | `docker-compose.yml` |
|---|---|---|---|

### Conceptos de Kafka que vas a ver en todas partes

| Palabra | Qué es | Analogía |
|---|---|---|
| **Topic** | Una categoría de mensajes. Aquí hay uno: `pedidos` | Una sección del periódico |
| **Partición** | `pedidos` está dividido en 3 "carriles" (0, 1, 2) para trabajar en paralelo | 3 cajas de supermercado |
| **Offset** | La posición de cada mensaje dentro de su carril: 0, 1, 2… | El número de página |
| **Consumer group** | Un "equipo lector" con su propio marcapáginas | Dos personas leyendo el mismo libro, cada una con su separador |
| **Lag** | Cuántos mensajes le faltan por leer a un equipo | Páginas que te faltan |

**La propiedad más importante de Kafka:** ❗ **leer un mensaje NO lo borra.** Se guarda durante días, y cada lector avanza a su ritmo. Si un lector se apaga, al volver **continúa donde iba**.

---

## Parte 3 — El mapa completo

```
╔══════════════════════════════════════════════════════════════════════════════════════════╗
║                              TU COMPUTADOR  (con Docker)                                  ║
║                                                                                          ║
║  ┌─────────────┐  pedidos   ┌──────────────────┐                ┌────────────────────┐   ║
║  │ 📱 productor │──────────► │   🟦 kafka        │──────────────► │ 🚨 consumidor-      │   ║
║  │  (carpeta    │  kafka:9092│  topic "pedidos" │   lee lo mismo │    alertas         │   ║
║  │  productor/) │            │  3 particiones   │                └────────────────────┘   ║
║  └─────────────┘            └────────┬─────────┘                                         ║
║                                      │ lee cada 10 s       ┌───────────────┐             ║
║                                      │                     │ 👁 kafka-ui    │◄── :8088    ║
║                                      ▼                     │ (mira Kafka)  │             ║
║                          ┌────────────────────────┐        └───────────────┘             ║
║                          │ ⚙️ spark-streaming      │                                      ║
║                          │  spark/jobs/streaming… │                                      ║
║                          └───────────┬────────────┘                                      ║
║                                      │ escribe                                           ║
║            ┌─────────────────────────▼──────────────────────────────────┐                ║
║            │ 🗄️ datalake/   (carpeta COMPARTIDA con tu PC)               │                ║
║            │   🥉 bronze/   🥈 silver/   🥇 gold/   📄 reportes/          │                ║
║            └─────────────────────────▲──────────────────────────────────┘                ║
║                                      │ lee silver, escribe gold y reportes               ║
║                          ┌───────────┴────────────┐                                      ║
║                          │ 🟩 airflow              │◄── :8089  (admin / admin)            ║
║                          │  cada 10 min ejecuta   │                                      ║
║                          │  airflow/dags/… que    │                                      ║
║                          │  lanza spark/jobs/gold…│                                      ║
║                          └────────────────────────┘                                      ║
╚══════════════════════════════════════════════════════════════════════════════════════════╝
```

### Las 7 cajas (contenedores)

| Contenedor | Qué hace | Carpeta con su código | ¿Siempre encendido? |
|---|---|---|---|
| `kafka` | El servidor de Kafka | — (imagen oficial) | ✅ Sí |
| `kafka-init` | Crea el topic `pedidos` **y se apaga** | — | ❌ No ("Exited (0)" es normal) |
| `kafka-ui` | Página web para **ver** Kafka → http://localhost:8088 | — | ✅ Sí |
| `productor` | Inventa pedidos | `productor/` | ✅ Sí |
| `consumidor-alertas` | Avisa de pedidos grandes o sucios | `productor/` | ✅ Sí |
| `spark-streaming` | Limpia en tiempo real → bronze + silver | `spark/jobs/` | ✅ Sí |
| `airflow` | Orquesta y lanza Spark batch → gold + reportes. Web → http://localhost:8089 | `airflow/dags/` + `spark/jobs/` | ✅ Sí |

---

## Parte 4 — CÓMO SE UNE TODO

Esta es la parte clave: **¿cómo es posible que piezas separadas trabajen juntas?** Hay **6 "pegamentos"**.

### 🔗 Pegamento 1: un solo plano, `docker-compose.yml`

Todo el proyecto se define en **un archivo**: `docker-compose.yml`. Es el **plano de la fábrica**. Para cada caja dice:

```yaml
productor:                        # ← nombre de la caja
  build: ./productor              # ← "constrúyela con la receta de la carpeta productor/"
  command: ["python", "-u", "productor.py"]   # ← "al encenderla, ejecuta esto"
  environment:
    KAFKA_BOOTSTRAP: kafka:9092   # ← "configuración: Kafka está en kafka:9092"
  depends_on:                     # ← "no la enciendas hasta que…"
    kafka-init:
      condition: service_completed_successfully   # "…se haya creado el topic"
```

Con **un solo comando** (`docker compose up -d`), Docker lee el plano y monta toda la fábrica.

### 🔗 Pegamento 2: la red interna, donde las cajas se llaman por su nombre

Docker crea una **red privada** donde cada caja se encuentra por su **nombre**, como los contactos de tu teléfono. Por eso el productor no necesita saber la "dirección IP" de Kafka: le basta con `kafka:9092`.

**¿Quién habla con quién, y por dónde?**

| Desde | Hacia | Dirección usada | Para qué |
|---|---|---|---|
| `productor` | `kafka` | `kafka:9092` | Enviar pedidos |
| `consumidor-alertas` | `kafka` | `kafka:9092` | Leer pedidos |
| `spark-streaming` | `kafka` | `kafka:9092` | Leer pedidos |
| `kafka-ui` | `kafka` | `kafka:9092` | Mostrar Kafka en la web |
| `kafka-init` | `kafka` | `kafka:9092` | Crear el topic |
| **Tu navegador** | `kafka-ui` | `localhost:8088` | Ver Kafka |
| **Tu navegador** | `airflow` | `localhost:8089` | Ver Airflow |
| **Un programa en tu PC** | `kafka` | `localhost:9094` | (opcional) Conectarte desde fuera de Docker |

> 🚪 **¿Por qué Kafka tiene dos direcciones (9092 y 9094)?** Las cajas están **dentro** de la red de Docker y usan `kafka:9092`. Tu PC está **fuera**: no conoce el nombre "kafka", así que Kafka abre una segunda puerta, `localhost:9094`, hacia afuera. Lo mismo pasa con las páginas web: `8088` y `8089` son "puertas" de tu PC hacia las cajas.

### 🔗 Pegamento 3: las carpetas compartidas (volúmenes)

Las cajas están aisladas, pero algunas carpetas de tu PC se **comparten** con ellas. Es como una carpeta de Google Drive que ven varias personas a la vez:

| Carpeta en tu PC | Dentro de la caja | La usan | Para qué |
|---|---|---|---|
| `datalake/` | `/opt/datalake` | `spark-streaming` y `airflow` | **¡Aquí se pasan los datos!** Spark Streaming escribe silver y Airflow lo lee |
| `spark/jobs/` | `/opt/jobs` | `spark-streaming` y `airflow` | El código de Spark |
| `airflow/dags/` | `/opt/airflow/dags` | `airflow` | La lista de tareas |

> 🧠 **Fíjate:** Spark Streaming y Airflow **nunca hablan directamente**. Se comunican **a través de la carpeta `datalake/`**: uno deja los datos limpios en `silver/` y el otro los recoge. Kafka hace lo mismo entre el productor y Spark. **Las piezas no se conocen: solo comparten un "lugar" donde dejar y recoger cosas.** A eso se le llama **desacoplar**.

### 🔗 Pegamento 4: los "acuerdos" entre piezas (contratos)

Para que todo encaje, ciertas cosas **deben coincidir exactamente** entre archivos distintos. Si cambias una, debes cambiar la otra:

| Acuerdo | Lo define… | …y debe coincidir con | Si no coinciden… |
|---|---|---|---|
| Nombre del topic: `pedidos` | `kafka-init` (en `docker-compose.yml`) | `productor.py`, `consumidor_alertas.py`, `streaming_pedidos.py` | Nadie lee lo que se escribe |
| Forma del pedido (campos JSON) | `productor.py` (`crear_pedido`) | `esquema` en `streaming_pedidos.py` | Spark ve las columnas vacías |
| Ruta de silver: `/opt/datalake/silver/pedidos` | `streaming_pedidos.py` | `gold_ventas.py` y `verificar_silver` en el DAG | Airflow dice "no hay datos" |
| Ruta de gold: `/opt/datalake/gold/...` | `gold_ventas.py` | `validar_calidad` y `publicar_reporte` en el DAG | La validación falla |

### 🔗 Pegamento 5: el orden de arranque

`docker-compose.yml` usa `depends_on` y `healthcheck` para que las cajas se enciendan **en el orden correcto**, aunque las arranques todas a la vez:

```
 kafka ──(cuando responde: "healthy")──► kafka-init ──(cuando termina: crea el topic)──┬──► productor ──► consumidor-alertas
   │                                                                                    └──► spark-streaming
   └──(healthy)──► kafka-ui

 airflow  (arranca por su cuenta; si aún no hay datos, su 1ª tarea lo detecta y para)
```

**¿Por qué este orden?**

| Orden | Pieza | Por qué va aquí |
|---|---|---|
| 1º | **Kafka** | Es la **puerta de entrada**. Todo pasa por él. Sin Kafka, los demás no tienen dónde escribir ni de dónde leer |
| 2º | **El topic** (`kafka-init`) | Hay que crear la "sección" antes de que alguien publique en ella |
| 3º | **Productor y consumidores** | Ya tienen dónde escribir y de dónde leer |
| 4º | **Spark Streaming** | Necesita que existan datos en Kafka para limpiarlos |
| 5º | **Airflow** | Necesita que existan datos limpios (silver) para hacer resúmenes. **Es el último en la cadena, pero el que manda** sobre las tareas programadas |

> 💡 **¿Y si enciendo Spark antes que el productor?** ¡No pasa nada! Spark espera a que lleguen mensajes. **¿Y si enciendo el productor y Spark una hora después?** ¡Tampoco pasa nada! Kafka guarda todos los pedidos y Spark los procesa al llegar. **Esta tolerancia es el gran beneficio de Kafka.**

### 🔗 Pegamento 6: dos ritmos que conviven

```
TIEMPO REAL  (nunca para)
 productor ──► kafka ──► spark-streaming ──► bronze + silver
   cada 0,5 s              cada 10 s

PROGRAMADO  (despierta y se duerme)
 airflow ──► spark batch ──► gold ──► reportes
   cada 10 minutos (en una empresa: 1 vez al día)
```

- La parte de **tiempo real** responde a "¿qué está pasando **ahora**?" (alertas, datos frescos).
- La parte **programada** responde a "¿qué pasó en **este periodo**?" (reportes, totales).

Casi todas las empresas combinan ambos ritmos.

### 🍕 El viaje completo de UN pedido

```
 00:00.0  📱 productor.py inventa: "cliente_001 pidió 3 pizzas en Barranquilla: $114.000"
 00:00.0  🟦 Kafka lo guarda → topic "pedidos", partición 2, offset 57      (productor/README)
 00:00.1  🚨 consumidor-alertas lo lee: "¿es mayor a $150.000? No → lo ignora"
 00:07.0  ⚙️ Spark Streaming lo lee en el siguiente micro-lote               (spark/jobs/README)
          ├─ 🥉 lo guarda tal cual en datalake/bronze/
          └─ 🥈 revisa: ¿tiene cliente? ✅  ¿total > 0? ✅ → lo guarda en datalake/silver/
 10:00.0  🟩 Airflow despierta (minuto :10)                                  (airflow/dags/README)
          ① verificar_silver: "hay datos" ✅
          ② lanza Spark batch: suma TODOS los pedidos de silver por ciudad
             → 🥇 datalake/gold/: "Barranquilla: 85 pedidos, $4.649.000"     (datalake/README)
          ③ validar_calidad: "¿ventas positivas? ¿ciudades llenas?" ✅
          ④ publicar_reporte → 📄 datalake/reportes/ventas_<fecha>.csv
```

---

## Parte 5 — Paso a paso: encender el pipeline

### Antes de empezar

**¿Qué necesitas?**
- **Docker** instalado. Compruébalo en una terminal:
  ```bash
  docker --version
  docker compose version
  ```
- **~2,5 GB de RAM libres para Docker** y **~4 GB de disco**.
- **Puertos libres:** 8088, 8089 y 9094.

**¿Cómo uso la terminal?**
1. Abre la aplicación **Terminal**.
2. Copia un comando (los bloques grises), pégalo con **`Ctrl + Shift + V`** y pulsa **`Enter`**.
3. **Todos los comandos se ejecutan desde la carpeta del proyecto.** Lo primero siempre es:
   ```bash
   cd ~/pipeline-datos
   ```
4. Los comandos con **`-f`** ("follow", seguir) **se quedan mostrando** información en vivo. Para salir pulsa **`Ctrl + C`**. Esto **no apaga nada**: solo dejas de mirar.

> ⚠️ **Si ya está todo encendido** y quieres vivir el proceso desde cero:
> ```bash
> cd ~/pipeline-datos
> docker compose down
> rm -rf datalake/bronze datalake/silver datalake/gold datalake/reportes datalake/_checkpoints
> ```

---

### ▶️ PASO 0 — Construir las cajas (solo la primera vez)

```bash
cd ~/pipeline-datos
docker compose build
```

**¿Qué hace?** Lee las recetas (`productor/Dockerfile` y `spark/Dockerfile`) y prepara las imágenes. La primera vez **tarda varios minutos** (descarga Java, Spark, Airflow…).

**✅ Deberías ver al final:**
```
 pipeline-spark-airflow:1.0  Built
 pipeline-productor:1.0  Built
```

---

### ▶️ PASO 1 — Encender Kafka (la cinta transportadora)

```bash
docker compose up -d kafka kafka-init kafka-ui
```
*(`up` = encender · `-d` = en segundo plano · luego vienen los nombres de las cajas)*

**Comprueba que se creó el topic:**
```bash
docker compose logs kafka-init
```

**✅ Deberías ver:**
```
Created topic pedidos.
Topic: pedidos  PartitionCount: 3  ReplicationFactor: 1
    Topic: pedidos  Partition: 0  Leader: 1 ...
    Topic: pedidos  Partition: 1  Leader: 1 ...
    Topic: pedidos  Partition: 2  Leader: 1 ...
```

**🧠 ¿Qué significa?** Kafka funciona y tiene una sección `pedidos` con 3 carriles. **Está vacía**: nadie ha enviado nada todavía.

👀 **Míralo:** abre http://localhost:8088 → **Topics** → `pedidos`. Verás **0 mensajes**.

---

### ▶️ PASO 2 — Encender el productor y el equipo de alertas

📖 *Lee mientras tanto:* [`productor/README.md`](productor/README.md)

```bash
docker compose up -d productor consumidor-alertas
```

**Mira al productor en vivo** (`Ctrl + C` para salir):
```bash
docker compose logs -f productor
```

**✅ Deberías ver** (una línea cada medio segundo):
```
-> Pedido 679b1185 | cliente_001  | Barranquilla | $    36000 guardado en particion=2 offset=2
-> Pedido 6efb0db2 | cliente_001  | Bogotá       | $    14000 guardado en particion=2 offset=3
-> Pedido d48945f2 | cliente_008  | Medellín     | $   156000 guardado en particion=1 offset=13
```

**🧠 ¿Qué significa?** Cada pedido se guardó en un **carril** (`particion`) y en una **posición** (`offset`). 🔎 `cliente_001` cae **siempre** en la partición 2: el cliente es la *key* que decide el carril.

**Mira al equipo de alertas** (`Ctrl + C` para salir):
```bash
docker compose logs -f consumidor-alertas
```

**✅ Deberías ver:**
```
[PEDIDO GRANDE] cliente_010 en Bogotá: 4x Sushi = $208,000
[DATO SUCIO] particion=1 offset=40 -> {... 'cliente': None ...}
```

**🧠 ¿Qué significa?** Dos programas que **no se conocen** usan los mismos datos: uno escribe en Kafka y el otro lee de Kafka.

👀 **Míralo:** en http://localhost:8088 → **Topics** → `pedidos` → **Messages**: ¡ahí están los pedidos! Recarga y verás cómo crecen.

> 🧠 **Ahora mismo Spark está apagado, pero los pedidos se están acumulando en Kafka.** **Espera 1 o 2 minutos** antes del Paso 3 para comprobarlo.

---

### ▶️ PASO 3 — Encender Spark Streaming (la limpieza en tiempo real)

📖 *Lee mientras tanto:* [`spark/README.md`](spark/README.md) y [`spark/jobs/README.md`](spark/jobs/README.md)

```bash
docker compose up -d spark-streaming
```

Spark tarda **20–40 segundos** en arrancar. Luego mira los micro-lotes (`Ctrl + C` para salir):
```bash
docker compose logs -f spark-streaming | grep Micro
```

**✅ Deberías ver:**
```
[Micro-lote 0] leidos de Kafka: 358 (offsets 0-138) | bronze: +358 | silver: +351 | descartados (sucios): 7
[Micro-lote 1] leidos de Kafka: 10 (offsets 82-144) | bronze: +10 | silver: +9 | descartados (sucios): 1
[Micro-lote 2] leidos de Kafka: 18 (offsets 85-150) | bronze: +18 | silver: +17 | descartados (sucios): 1
```

**🧠 ¿Qué significa? (¡lo más importante de todo el proyecto!)**
- **El micro-lote 0 leyó cientos de pedidos de golpe:** son los que se **acumularon en Kafka mientras Spark estaba apagado**. ¡No se perdió ninguno!
- Después, **~20 por micro-lote** (2 por segundo × 10 segundos).
- `bronze: +358` → guardó **todo**. `silver: +351` → solo los **limpios**. `descartados: 7` → tenían errores.

**Mira cómo se llena el almacén** 📖 [`datalake/README.md`](datalake/README.md):
```bash
ls datalake/
ls datalake/silver/pedidos/
```
Aparecen `bronze/`, `silver/` y una carpeta `fecha=AAAA-MM-DD`.

---

### ▶️ PASO 4 — Encender Airflow (el jefe de planta)

📖 *Lee mientras tanto:* [`airflow/README.md`](airflow/README.md) y [`airflow/dags/README.md`](airflow/dags/README.md)

```bash
docker compose up -d airflow
```

Tarda **~1 minuto** en arrancar. Para saber si está listo:
```bash
curl -s localhost:8089/health
```
Si la respuesta contiene `"healthy"`, abre:

👉 **http://localhost:8089** · Usuario **`admin`** · Contraseña **`admin`**

**En la página:**
1. Haz clic en **`pipeline_pedidos`**.
2. Pulsa el botón **▶ (Trigger DAG)** arriba a la derecha para ejecutarlo **ahora** (si no, se ejecuta solo cada 10 minutos).
3. Ve a la pestaña **Graph** y mira cómo las 4 cajitas se ponen 🟢 **verdes**, una tras otra.
4. Haz clic en la cajita `spark_calcular_gold` → **Logs**: verás la tabla que calculó Spark.

**O desde la terminal:**
```bash
docker exec airflow airflow dags trigger pipeline_pedidos
docker exec airflow airflow dags list-runs -d pipeline_pedidos
```

**✅ Deberías ver** `success` en la columna `state` (tarda ~20–30 segundos).

**🧠 ¿Qué pasó?** Airflow:
① comprobó que había datos en silver → ② le **ordenó a Spark** calcular las ventas → ③ revisó que los números tuvieran sentido → ④ guardó el reporte.

---

### ▶️ PASO 5 — Ver el resultado final 🎉

```bash
ls datalake/reportes/
column -s, -t < "$(ls datalake/reportes/*.csv | tail -1)"
```

**✅ Deberías ver:**
```
fecha       ciudad        num_pedidos  ventas_totales  ticket_promedio  clientes_unicos
2026-10-14  Cartagena     106          7524000         70981.0          44
2026-10-14  Bogotá        101          6476000         64119.0          42
2026-10-14  Medellín      101          5999000         59396.0          45
2026-10-14  Cali          99           5654000         57111.0          44
2026-10-14  Barranquilla  85           4649000         54694.0          41
```

O ábrelo con **Excel / LibreOffice**: en el explorador de archivos ve a `pipeline-datos/datalake/reportes/` y haz doble clic.

✅ **¡Acabas de ver un pipeline de datos completo!** Los pedidos nacieron en una app, viajaron por Kafka, Spark los limpió y resumió, y Airflow coordinó todo hasta producir un reporte.

---

## Parte 6 — Experimentos para entender de verdad

Rompe cosas **a propósito** y mira cómo reacciona el sistema. Es la mejor forma de aprender.

### 🧪 1. "Kafka no pierde nada" (el más importante)

```bash
docker compose stop spark-streaming            # apagamos la máquina
docker compose logs --tail 5 productor         # …pero el productor SIGUE enviando
```
Espera **1 minuto** y vuelve a encender:
```bash
docker compose start spark-streaming
docker compose logs -f spark-streaming | grep Micro
```
**Resultado:** el primer micro-lote lee **~120 pedidos de golpe** (los de ese minuto). Spark continuó **exactamente donde iba** gracias a su marcapáginas (`datalake/_checkpoints/`).

### 🧪 2. "El lag": cuánto le falta a un lector

```bash
docker compose stop consumidor-alertas
```
Espera 30 segundos y mira:
```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group equipo-alertas
```
La columna **LAG** muestra cuántos pedidos **aún no ha leído**, y crece con el tiempo. Ahora enciéndelo (`docker compose start consumidor-alertas`) y repite el comando: el LAG baja casi a **0**. ¡Se puso al día!

### 🧪 3. Hora pico (más tráfico)

En `docker-compose.yml` cambia `PEDIDOS_POR_SEGUNDO: "2"` por `"10"`, guarda y ejecuta:
```bash
docker compose up -d productor
docker compose logs -f spark-streaming | grep Micro
```
Los micro-lotes pasan de ~20 a ~100 pedidos. Spark aguanta sin problemas. Vuelve a dejarlo en `"2"` después.

### 🧪 4. Airflow detecta un fallo y Kafka permite reconstruir

```bash
docker compose stop spark-streaming
rm -rf datalake/silver
docker exec airflow airflow dags trigger pipeline_pedidos
```
En Airflow → **Graph**: `verificar_silver` se pone 🟠 (reintenta) y luego 🔴 **rojo**. Las demás tareas **no se ejecutan** (🟤). En sus **Logs** verás *"No hay datos en silver todavía"*.

**Para reconstruir:**
```bash
rm -rf datalake/_checkpoints      # Spark "olvida" por dónde iba
docker compose start spark-streaming
docker compose logs -f spark-streaming | grep Micro
```
**Resultado:** Spark **relee TODO Kafka desde el principio** y reconstruye silver. **Esto es posible porque Kafka no borra los mensajes al leerlos.**

> 📝 Bronze quedará con pedidos repetidos (se escribieron dos veces). No afecta a gold, que cuenta pedidos *distintos*.

---

## Parte 7 — Apagar, encender y empezar de cero

| Quiero… | Comando | ¿Qué pasa con los datos? |
|---|---|---|
| Ver qué está encendido | `docker compose ps -a` | Nada |
| **Pausar** todo (liberar RAM) | `docker compose stop` | Se conserva **todo** |
| **Reanudar** | `docker compose start` | Continúa donde iba |
| Encender todo de una vez | `docker compose up -d` | Arranca en el orden correcto (Pegamento 5) |
| **Apagar y quitar las cajas** | `docker compose down` | ⚠️ Kafka **pierde** sus mensajes. `datalake/` se conserva |
| **Empezar 100% de cero** | `docker compose down` y luego `rm -rf datalake/bronze datalake/silver datalake/gold datalake/reportes datalake/_checkpoints` | Se borra todo |
| Recuperar ~4 GB de disco | `docker compose down --rmi all` | Habrá que repetir `docker compose build` |

> ⚠️ **Después de `docker compose down`, vacía siempre el datalake** (comando de "empezar de cero"). Si no, el marcapáginas de Spark apuntará a mensajes de un Kafka que ya no existe.
>
> ⚠️ No uses `rm -rf datalake/*`: borrarías también `datalake/README.md`.

---

## Parte 8 — Si algo falla

**1) Mira el estado de las cajas:**
```bash
docker compose ps -a
```
- `Up` → funcionando ✅
- `Exited (0)` en `kafka-init` → **normal** ✅
- `Exited (1)`, `(2)`, `(137)`… en otra → falló ❌

**2) Mira sus últimas 50 líneas de log:**
```bash
docker compose logs --tail 50 NOMBRE_DE_LA_CAJA
```

| Problema | Causa probable | Solución |
|---|---|---|
| `Exited (137)` | **Sin memoria**: Docker mató la caja | `docker compose stop` las cajas que no uses, o dale más RAM a Docker Desktop (**Settings → Resources → Memory**). `spark-streaming` se reinicia solo |
| `port is already allocated` | Otro programa usa ese puerto | En `docker-compose.yml`, cambia el número de la **izquierda** en `ports:` (por ejemplo `"8090:8080"`) |
| `spark-streaming` reinicia en bucle | Marcapáginas viejo tras un `down` | `docker compose stop spark-streaming`, `rm -rf datalake/_checkpoints`, `docker compose start spark-streaming` |
| Airflow no abre en el navegador | Aún está arrancando | Espera 1–2 minutos y recarga |
| `verificar_silver` en 🔴 | Spark Streaming no está funcionando | Revisa el Paso 3 |
| "Broken DAG" en Airflow | Error de escritura en un `.py` de `airflow/dags/` | `docker exec airflow airflow dags list-import-errors` |
| `permission denied` al borrar `datalake/` | Archivos creados por una caja | `docker compose down`, luego `sudo rm -rf datalake/bronze …` |
| `Cannot connect to the Docker daemon` | Docker no está abierto | Abre Docker Desktop y espera a que diga "running" |

---

## Parte 9 — ¿Y RabbitMQ?

| | 🟦 **Kafka** | 🐰 **RabbitMQ** |
|---|---|---|
| Analogía | 📰 Un **periódico con hemeroteca** | 📮 Un **cartero** |
| Al leer un mensaje… | **Se queda** guardado | **Desaparece** |
| ¿Varios pueden leer lo mismo? | ✅ Sí, cada uno con su marcapáginas | Solo si se copia a varias colas |
| ¿Se puede releer el pasado? | ✅ Sí (lo hiciste en el Experimento 4) | ❌ No |
| Ideal para | **Datos y eventos** que muchos analizan | **Tareas** puntuales: "envía este email" |

**En este proyecto usamos Kafka** porque varios sistemas (alertas y Spark) leen **los mismos pedidos**, y queremos poder **reprocesarlos**. RabbitMQ encajaría para algo como "cuando haya un pedido grande, **envía un SMS** al gerente": una tarea que se hace **una vez** y se olvida.

---

## Parte 10 — Glosario

| Término | Explicación sencilla |
|---|---|
| **Pipeline de datos** | Cadena de pasos que lleva los datos desde donde nacen hasta donde se usan |
| **Evento** | Un hecho que ocurrió: "se creó un pedido" |
| **Productor / Consumidor** | Quien **escribe** en Kafka / quien **lee** de Kafka |
| **Topic / Partición / Offset** | Sección / carril / número de posición dentro de Kafka |
| **Consumer group** | Equipo lector con su propio marcapáginas |
| **Lag** | Mensajes que un lector aún no ha leído |
| **Streaming** | Procesar los datos **a medida que llegan** |
| **Batch** | Procesar **un montón de datos de una vez**, cada cierto tiempo |
| **Micro-lote** | Cada "ronda" de Spark Streaming (aquí, cada 10 s) |
| **Checkpoint** | El marcapáginas de Spark, guardado en disco |
| **DataFrame** | Una tabla de datos dentro de Spark (como una hoja de Excel) |
| **Data lake** | Almacén de archivos con datos de todo tipo |
| **Bronze / Silver / Gold** | Crudo / limpio / resumido |
| **Parquet** | Formato de archivo comprimido y rápido para datos |
| **DAG** | Diagrama de tareas con flechas (Airflow) |
| **Task / Operator** | Una cajita del DAG / el tipo de cajita (Python, Bash…) |
| **Cron** | Formato para programar horarios: `*/10 * * * *` = cada 10 minutos |
| **Orquestar** | Coordinar qué se ejecuta, cuándo y en qué orden |
| **Imagen / Contenedor** | La receta de una caja / la caja funcionando |
| **Volumen** | Carpeta compartida entre tu PC y una caja |
| **Desacoplar** | Que las piezas no dependan directamente unas de otras |
| **JSON** | Texto con formato `{"clave": "valor"}` para enviar datos |

---

## Parte 11 — Chuleta de comandos

```bash
cd ~/pipeline-datos                                   # SIEMPRE primero

# --- Encender (en orden) ---
docker compose build                                  # construir (solo la 1ª vez)
docker compose up -d kafka kafka-init kafka-ui        # 1. Kafka
docker compose up -d productor consumidor-alertas     # 2. Pedidos entrando
docker compose up -d spark-streaming                  # 3. Spark tiempo real
docker compose up -d airflow                          # 4. Airflow
docker compose up -d                                  # …o TODO de una vez

# --- Mirar ---
docker compose ps -a                                  # ¿qué está encendido?
docker stats --no-stream                              # ¿cuánta memoria usa cada caja?
docker compose logs -f productor                      # pedidos enviados      (Ctrl+C para salir)
docker compose logs -f consumidor-alertas             # alertas               (Ctrl+C para salir)
docker compose logs -f spark-streaming | grep Micro   # micro-lotes de Spark  (Ctrl+C para salir)
column -s, -t < "$(ls datalake/reportes/*.csv | tail -1)"   # último reporte

# --- Kafka por dentro ---
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic pedidos
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group equipo-alertas

# --- Airflow ---
docker exec airflow airflow dags trigger pipeline_pedidos       # ejecutar ya
docker exec airflow airflow dags list-runs -d pipeline_pedidos  # historial

# --- Spark a mano ---
docker exec airflow spark-submit --master 'local[2]' --driver-memory 512m /opt/jobs/gold_ventas.py

# --- Apagar ---
docker compose stop                                   # pausar (conserva todo)
docker compose down                                   # apagar y quitar cajas
rm -rf datalake/bronze datalake/silver datalake/gold datalake/reportes datalake/_checkpoints   # vaciar almacén
```

**Páginas web:**
- 👁 **Kafka UI** → http://localhost:8088
- 🟩 **Airflow** → http://localhost:8089 (usuario `admin`, contraseña `admin`)
