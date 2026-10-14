# 🟩 Carpeta `airflow/`: el JEFE DE PLANTA

> **En una frase:** Airflow **no toca los datos**. Su trabajo es decir **qué tareas se ejecutan, cuándo y en qué orden**, vigilar que salgan bien y avisar si algo falla.

⬅️ [Volver al README principal](../README.md)

---

## 1. ¿Qué es Airflow? (sin tecnicismos)

Imagina al **jefe de cocina de un restaurante** con una lista pegada en la pared:

```
 Todos los días a las 6:00 a. m.:
   1. Revisar que llegaron los ingredientes      → si NO llegaron, PARAR y avisar
   2. Preparar las salsas
   3. Probar que las salsas saben bien           → si saben mal, PARAR y avisar
   4. Poner el menú del día en la puerta
```

El jefe **no cocina**: **vigila que cada paso se haga, en orden y a tiempo**. Si un paso falla, no deja que se haga el siguiente (¡no vas a poner el menú si las salsas salieron mal!).

**Airflow es ese jefe, pero para tareas de datos.** Y la "lista en la pared" es un archivo Python llamado **DAG**.

### ¿Por qué no basta con un programa normal?

Podrías escribir un script que haga los 4 pasos seguidos. Pero Airflow además te da:

| Lo que te da Airflow | Ejemplo en este proyecto |
|---|---|
| ⏰ **Ejecución programada** | Corre solo cada 10 minutos, sin que nadie lo lance |
| 🔗 **Orden y dependencias** | Si `verificar_silver` falla, Spark no se ejecuta |
| 🔁 **Reintentos automáticos** | Si una tarea falla, la reintenta 1 vez a los 30 segundos |
| 📜 **Historial y logs** | Puedes ver qué pasó en cada ejecución, aunque haya sido ayer |
| 🖥️ **Una página web** | Ves todo con colores, sin comandos |
| 🚨 **Alertas** | En una empresa, te enviaría un email o un mensaje de Slack si algo falla |

---

## 2. ¿Qué papel cumple en el pipeline?

```
 🟦 KAFKA ──► ⚙️ SPARK STREAMING ──► 🥉 bronze  🥈 silver
                                                   │
           ┌───────────────────────────────────────┼──────────────────┐
           │ 🟩 AIRFLOW  (cada 10 minutos)          ▼                  │ ◄── ESTÁS AQUÍ
           │                                                          │
           │  ① verificar_silver ─► ② spark_calcular_gold ─►          │
           │                         (lanza Spark batch)              │
           │  ③ validar_calidad  ─► ④ publicar_reporte                │
           └──────────────────────────────────┬───────────────────────┘
                                              ▼
                                🥇 gold   📄 reportes/ventas_<fecha>.csv
```

> 🧠 **Fíjate:** Kafka, el productor y Spark Streaming trabajan **sin parar** (tiempo real). Airflow **despierta cada cierto tiempo** (programado). Son dos ritmos distintos que conviven.

### ¿Por qué Airflow va AL FINAL de la cadena?

Porque **depende** de que lo anterior funcione. Su primera tarea (`verificar_silver`) comprueba que Spark Streaming ya dejó datos limpios. Sin datos, no hay nada que orquestar.

Pero aunque se monta al final, **es el que manda** sobre los procesos programados.

---

## 3. ¿Qué archivos hay?

```
airflow/
├── README.md            ← este archivo
└── dags/                ← aquí van las "listas de tareas" (DAGs)
    ├── README.md        ← explicación del DAG, tarea por tarea
    └── pipeline_pedidos.py   ← nuestro DAG
```

Airflow **revisa automáticamente** la carpeta `dags/` cada 15 segundos. Si creas un archivo `.py` nuevo con un DAG, **aparece solo** en la página web. No hay que reiniciar nada.

> ¿Cómo la ve Airflow? Gracias a la carpeta compartida de `docker-compose.yml`:
> `./airflow/dags:/opt/airflow/dags` → tu carpeta `airflow/dags` es la carpeta `/opt/airflow/dags` dentro de la caja.

---

## 4. Las partes de Airflow (lo que corre dentro del contenedor)

Usamos el modo **`standalone`** ("todo en uno"), ideal para aprender. Dentro del contenedor `airflow` funcionan varias piezas:

| Pieza | Qué hace | Analogía |
|---|---|---|
| **Scheduler** (programador) | Mira el reloj y los DAGs, y decide qué toca ejecutar | El jefe mirando el reloj |
| **Executor** (ejecutor) | Ejecuta las tareas | Los cocineros |
| **Webserver** (servidor web) | La página http://localhost:8089 | La pizarra donde se ve el estado |
| **Base de datos** (SQLite) | Guarda el historial: qué se ejecutó, cuándo y cómo salió | El cuaderno de registro |

> 🏢 En una empresa real, cada pieza va en un servidor distinto, con una base de datos PostgreSQL y varios "trabajadores" en paralelo. **El DAG que escribas sería exactamente el mismo.**

---

## 5. Entrar a la página web, paso a paso

1. Asegúrate de que está encendido: `docker compose ps` → `airflow` debe decir `Up`.
2. Abre **http://localhost:8089** en el navegador.
3. Usuario: **`admin`** · Contraseña: **`admin`**.

### Lo que ves en la pantalla principal

```
 DAGs
 ┌────┬──────────────────┬────────────┬──────────────┬────────────┬─────────┐
 │ ⏽  │ DAG              │ Schedule   │ Last Run     │ Runs       │ Actions │
 ├────┼──────────────────┼────────────┼──────────────┼────────────┼─────────┤
 │ 🔵 │ pipeline_pedidos │ */10 * * * │ 2026-10-14…  │ 🟢 🟢 🟢    │   ▶  🗑  │
 └────┴──────────────────┴────────────┴──────────────┴────────────┴─────────┘
   │                                                                 │
   └─ interruptor: azul = activo (se ejecuta solo)                    └─ ▶ = ejecutar YA
```

### Las pestañas dentro del DAG (haz clic en `pipeline_pedidos`)

| Pestaña | Qué muestra | Para qué te sirve |
|---|---|---|
| **Grid** | Una cuadrícula: cada **columna** es una ejecución y cada **fila** una tarea | Ver el historial de un vistazo |
| **Graph** | El diagrama con las 4 cajitas y sus flechas | Entender el orden |
| **Code** | El código Python del DAG | Ver la "lista en la pared" |

### Los colores

| Color | Estado | Significado |
|---|---|---|
| ⚪ Blanco/gris | `queued` / `scheduled` | En espera de su turno |
| 🟢 Verde claro | `running` | Ejecutándose ahora |
| 🟢 **Verde oscuro** | `success` | ✅ Terminó bien |
| 🟠 Naranja | `up_for_retry` | Falló, pero lo va a reintentar |
| 🔴 **Rojo** | `failed` | ❌ Falló definitivamente |
| 🟤 Naranja oscuro | `upstream_failed` | No se ejecutó porque **una tarea anterior falló** |

### Ver qué hizo una tarea por dentro
Haz clic en un **cuadrito** (en Grid o Graph) → pestaña **Logs**. Ahí está todo lo que imprimió la tarea. Por ejemplo, en `spark_calcular_gold` verás la tabla de ventas que calculó Spark.

---

## 6. Comandos útiles (desde la terminal)

Ejecútalos desde la carpeta del proyecto (`cd ~/pipeline-datos`):

```bash
# Encender Airflow (tarda ~1 minuto en estar listo)
docker compose up -d airflow

# ¿Ya está listo? (debe responder con "healthy")
curl -s localhost:8089/health

# Ver los DAGs que encontró
docker exec airflow airflow dags list

# Ejecutar el DAG ahora mismo
docker exec airflow airflow dags trigger pipeline_pedidos

# Ver el historial de ejecuciones
docker exec airflow airflow dags list-runs -d pipeline_pedidos

# Pausar / reanudar la ejecución automática
docker exec airflow airflow dags pause pipeline_pedidos
docker exec airflow airflow dags unpause pipeline_pedidos

# ¿Hay errores en algún DAG? (por ejemplo, un error de escritura en el .py)
docker exec airflow airflow dags list-import-errors

# Ver los logs internos de Airflow
docker compose logs --tail 50 airflow
```

---

## 7. Configuración (en `docker-compose.yml`)

| Variable | Valor | Qué hace |
|---|---|---|
| `AIRFLOW__CORE__LOAD_EXAMPLES` | `false` | No carga los ~50 DAGs de ejemplo que trae Airflow |
| `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `false` | El DAG arranca **activado** (normalmente arranca pausado, por seguridad) |
| `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` | `15` | Busca DAGs nuevos cada 15 segundos |
| `AIRFLOW__WEBSERVER__WORKERS` | `1` | Un solo proceso para la web (ahorra memoria) |

> 💡 **El truco de los nombres:** `AIRFLOW__SECCION__OPCION` (con **doble** guion bajo) es la forma de cambiar cualquier configuración de Airflow mediante variables.

---

## 8. Resumen

- ✅ Airflow es un **orquestador**: decide **qué, cuándo y en qué orden**. No procesa datos.
- ✅ La lista de tareas se llama **DAG** y es un archivo Python en `dags/`.
- ✅ Corre **cada 10 minutos** sin que nadie lo lance, y también puedes lanzarlo a mano con ▶.
- ✅ Si una tarea falla, **las siguientes no se ejecutan** y lo ves en **rojo**.
- ✅ La página web (http://localhost:8089, admin/admin) muestra todo con colores y logs.
- ➡️ **Siguiente:** [`dags/README.md`](dags/README.md) explica el DAG tarea por tarea.
