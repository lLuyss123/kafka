# 📋 Carpeta `airflow/dags/`: la lista de tareas

> **En una frase:** aquí está `pipeline_pedidos.py`, el archivo que le dice a Airflow **"cada 10 minutos, haz estas 4 tareas, en este orden"**.

⬅️ [Volver a `airflow/`](../README.md) · [Volver al README principal](../../README.md)

---

## 1. ¿Qué es un DAG?

**DAG** = *Directed Acyclic Graph* (Grafo Dirigido Acíclico). Suena muy técnico, pero significa algo simple:

- **Grafo**: cajitas unidas con flechas.
- **Dirigido**: las flechas tienen sentido (A → B: primero A, luego B).
- **Acíclico**: **no hay vueltas en círculo** (nunca A → B → A). Si no, ¡el trabajo nunca terminaría!

En resumen: **un DAG es un diagrama de tareas con flechas que dicen el orden.**

Nuestro DAG:

```
 ┌──────────────────┐     ┌──────────────────────┐     ┌──────────────────┐     ┌──────────────────┐
 │ ① verificar_     │ ──► │ ② spark_calcular_    │ ──► │ ③ validar_       │ ──► │ ④ publicar_      │
 │    silver        │     │    gold              │     │    calidad       │     │    reporte       │
 │                  │     │                      │     │                  │     │                  │
 │ ¿Hay datos       │     │ Spark: silver → gold │     │ ¿Los números     │     │ Copia el CSV a   │
 │ limpios?         │     │ (ventas por ciudad)  │     │ tienen sentido?  │     │ reportes/ con    │
 │                  │     │                      │     │                  │     │ fecha y hora     │
 │ Python           │     │ Bash (spark-submit)  │     │ Python (pandas)  │     │ Python           │
 └──────────────────┘     └──────────────────────┘     └──────────────────┘     └──────────────────┘
```

---

## 2. `pipeline_pedidos.py`, parte por parte

### Parte A: la "ficha" del DAG

```python
with DAG(
    dag_id="pipeline_pedidos",                  # el nombre que ves en la web
    description="Silver -> Gold con Spark, validación y reporte",
    start_date=datetime(2026, 1, 1),            # desde cuándo es válido
    schedule="*/10 * * * *",                    # ⏰ CADA 10 MINUTOS
    catchup=False,                              # no "recuperar" ejecuciones pasadas
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},  # si falla, reintenta 1 vez a los 30 s
    tags=["aprendizaje", "spark", "kafka"],     # etiquetas para buscar en la web
) as dag:
```

#### ⏰ ¿Cómo se lee `*/10 * * * *`?

Es el formato **cron**, el estándar para programar tareas. Son 5 posiciones:

```
 */10    *      *      *      *
  │      │      │      │      └── día de la semana  (* = cualquiera)
  │      │      │      └───────── mes               (* = cualquiera)
  │      │      └──────────────── día del mes       (* = cualquiera)
  │      └─────────────────────── hora              (* = cualquiera)
  └────────────────────────────── minuto            (*/10 = cada 10: 0, 10, 20, 30, 40, 50)
```

Otros ejemplos:

| Cron | Significado |
|---|---|
| `0 2 * * *` | Todos los días a las **2:00 a. m.** (el más común en empresas) |
| `0 * * * *` | Cada hora, en punto |
| `0 8 * * 1` | Los **lunes** a las 8:00 a. m. |
| `@daily` | Una vez al día (atajo de `0 0 * * *`) |

> ⚠️ Airflow usa la hora **UTC** (5 horas más que Colombia). `0 2 * * *` en UTC son las 9:00 p. m. en Colombia.

#### ¿Qué es `catchup=False`?
`start_date` es el 1 de enero de 2026. Si `catchup` fuera `True`, Airflow intentaría ejecutar **todas** las ejecuciones "atrasadas" desde esa fecha: ¡miles! Con `False` solo ejecuta a partir de ahora.

### Parte B: las 4 tareas

#### ① `verificar_silver`: ¿hay ingredientes?

```python
def verificar_silver():
    archivos = glob.glob(f"{LAKE}/silver/pedidos/**/*.parquet", recursive=True)
    if not archivos:
        raise ValueError("No hay datos en silver todavía. ¿Está corriendo spark-streaming?")
    print(f"OK: {len(archivos)} archivos parquet en silver.")

t1 = PythonOperator(task_id="verificar_silver", python_callable=verificar_silver)
```

- Busca archivos `.parquet` dentro de `datalake/silver/pedidos`.
- **Si no hay ninguno**, `raise` **lanza un error a propósito**. Eso hace que la tarea salga en 🔴 rojo y **detiene todo el DAG**. ¿Para qué ejecutar Spark si no hay nada que procesar?
- **`PythonOperator`**: "esta tarea es una **función de Python**".

#### ② `spark_calcular_gold`: que trabaje la máquina

```python
t2 = BashOperator(
    task_id="spark_calcular_gold",
    bash_command="spark-submit --master local[2] --driver-memory 512m --conf spark.ui.enabled=false /opt/jobs/gold_ventas.py",
)
```

- **`BashOperator`**: "esta tarea es un **comando de terminal**".
- Ejecuta `spark-submit`, que lanza el programa de Spark [`gold_ventas.py`](../../spark/jobs/README.md).
- **Aquí se ve clarísimo el papel de Airflow:** no calcula nada él mismo. **Le ordena a Spark** que lo haga, espera a que termine y anota si salió bien o mal.

> 🏢 En una empresa se usaría `SparkSubmitOperator` para enviarlo a un clúster de Spark separado. Usamos `BashOperator` porque es más fácil de entender: es literalmente el mismo comando que escribirías en la terminal.

#### ③ `validar_calidad`: probar la salsa antes de servir

```python
def validar_calidad():
    df = pd.read_parquet(f"{LAKE}/gold/ventas_por_ciudad")
    print(df.to_string(index=False))
    assert len(df) > 0, "La tabla gold está vacía"
    assert (df["ventas_totales"] > 0).all(), "Hay ventas negativas: la limpieza falló"
    assert df["ciudad"].notna().all(), "Hay ciudades vacías"
```

- Lee el resultado de Spark con **pandas** (una librería de Python para manejar tablas).
- **`assert`** significa "**asegúrate de que** esto sea verdad; si no, **falla** con este mensaje". Revisa tres cosas:
  1. Que la tabla **no esté vacía**.
  2. Que **ninguna venta sea negativa** (si lo fuera, la limpieza de Spark habría fallado).
  3. Que **ninguna ciudad esté vacía**.
- Esto se llama **control de calidad de datos**. En una empresa es crucial: es mejor **no publicar un reporte** que publicar uno **con números equivocados**.

#### ④ `publicar_reporte`: poner el menú en la puerta

```python
def publicar_reporte(ts_nodash, **_):
    origen = glob.glob(f"{LAKE}/gold/ventas_por_ciudad_csv/part-*.csv")[0]
    destino = f"{LAKE}/reportes/ventas_{ts_nodash}.csv"
    shutil.copy(origen, destino)
```

- Busca el CSV que creó Spark. Spark le pone un nombre feo, como `part-00000-b2ce…csv`.
- Lo **copia** a `datalake/reportes/` con un nombre claro: `ventas_20261014T150337.csv`.
- **`ts_nodash`**: Airflow le **regala** a la función la fecha y hora de la ejecución (`20261014T150337`). Así cada reporte tiene un nombre único y no se sobrescriben.

### Parte C: ⭐ el orden (la línea más importante)

```python
t1 >> t2 >> t3 >> t4
```

El símbolo **`>>`** se lee **"y después"**:

> *verificar_silver **y después** spark_calcular_gold **y después** validar_calidad **y después** publicar_reporte.*

Con esto, Airflow sabe que:
- `t2` **no empieza** hasta que `t1` termine **bien**;
- si `t1` falla, `t2`, `t3` y `t4` quedan como `upstream_failed` (🟤 no se ejecutan).

**Otros órdenes posibles** (solo como ejemplo):
```python
t1 >> [t2, t3] >> t4     # t2 y t3 EN PARALELO, y t4 cuando AMBOS terminen
```
```
        ┌─► t2 ─┐
   t1 ──┤       ├──► t4
        └─► t3 ─┘
```

---

## 3. ¿Qué pasa en una ejecución? (línea de tiempo real)

Esto ocurrió en una ejecución real de este proyecto:

```
15:03:37  ⏰ Alguien pulsa ▶ (o llega el minuto :00, :10, :20…)
15:03:40  ① verificar_silver      → "OK: <N> archivos parquet en silver."          🟢
15:03:42  ② spark_calcular_gold   → Spark arranca, calcula y muestra la tabla        🟢
15:03:55  ③ validar_calidad       → "Calidad OK: 5 filas, ventas totales = $30,302,000" 🟢
15:03:58  ④ publicar_reporte      → "Reporte publicado en .../ventas_20261014T150337.csv" 🟢
15:03:59  ✅ DAG completo (22 segundos en total)
```

---

## 4. 🧪 Ejercicio: añade tu propia tarea

Vamos a añadir una 5ª tarea que diga cuántos reportes se han generado.

**1.** Abre `pipeline_pedidos.py` y, **antes** de la línea `with DAG(`, añade esta función:

```python
def contar_reportes():
    n = len(glob.glob(f"{LAKE}/reportes/*.csv"))
    print(f"Ya hay {n} reportes generados.")
```

**2.** Dentro del bloque `with DAG(...)`, después de `t4 = ...`, añade:

```python
    t5 = PythonOperator(task_id="contar_reportes", python_callable=contar_reportes)
```

**3.** Cambia la línea del orden:

```python
    t1 >> t2 >> t3 >> t4 >> t5
```

**4.** Guarda el archivo. En **~15–30 segundos**, Airflow lo detecta solo. Recarga la página → **Graph**: ¡verás 5 cajitas!

**5.** Pulsa ▶ y mira los **Logs** de `contar_reportes`.

> ❗ Si cometes un error de escritura, aparecerá un aviso rojo **"Broken DAG"** arriba de la página. Revisa el mensaje o ejecuta:
> `docker exec airflow airflow dags list-import-errors`

---

## 5. Resumen

- ✅ Un **DAG** es un diagrama de tareas con flechas. El nuestro tiene 4 tareas.
- ✅ `schedule="*/10 * * * *"` → se ejecuta **solo** cada 10 minutos (formato cron).
- ✅ **`PythonOperator`** ejecuta una función de Python; **`BashOperator`** ejecuta un comando de terminal.
- ✅ **`t1 >> t2 >> t3 >> t4`** define el orden. Si una tarea falla, las siguientes no se ejecutan.
- ✅ Airflow **no procesa datos**: le **ordena** a Spark que lo haga y **verifica** el resultado.
- ➡️ **Siguiente parada:** [`datalake/`](../../datalake/README.md), donde termina todo.
