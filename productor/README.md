# 📱 Carpeta `productor/`: donde NACEN los datos

> **En una frase:** aquí vive una **app de domicilios falsa** que inventa pedidos sin parar y los envía a Kafka, y un segundo programa que **lee esos mismos pedidos** para avisar de los más grandes.

⬅️ [Volver al README principal](../README.md)

---

## 1. ¿Qué papel cumple en el pipeline?

```
  ┌───────────────────┐
  │   📱 PRODUCTOR     │  ◄── ESTÁS AQUÍ (inventa pedidos)
  └─────────┬─────────┘
            │ envía cada pedido
            ▼
  ┌───────────────────┐        ┌──────────────────────────┐
  │  🟦 KAFKA          │──────► │ 🚨 CONSUMIDOR DE ALERTAS  │ ◄── TAMBIÉN AQUÍ (lee pedidos)
  │  topic "pedidos"  │        └──────────────────────────┘
  └─────────┬─────────┘
            ▼
       Spark, Airflow… (otras carpetas)
```

Todo pipeline de datos necesita una **fuente**: algo que genere los datos. En una empresa real serían:

- la **app** del celular registrando cada compra,
- **sensores** enviando temperaturas,
- una **página web** registrando cada clic,
- una **base de datos** avisando de cada cambio.

Como aquí no tenemos clientes reales, **los simulamos** con `productor.py`.

---

## 2. ¿Qué archivos hay?

| Archivo | Qué es | Analogía |
|---|---|---|
| `Dockerfile` | La **receta** para construir la caja donde corren estos programas | La lista de ingredientes |
| `productor.py` | El programa que **inventa y envía** pedidos a Kafka | El **periodista** que escribe noticias |
| `consumidor_alertas.py` | El programa que **lee** pedidos de Kafka y avisa de los grandes | Un **lector** del periódico |

Los dos programas usan **la misma caja** (la misma imagen de Docker), pero en `docker-compose.yml` se encienden como **dos contenedores distintos**: `productor` y `consumidor-alertas`.

---

## 3. `Dockerfile`, línea por línea

```dockerfile
FROM python:3.12-slim                           # 1
WORKDIR /app                                    # 2
RUN pip install --no-cache-dir confluent-kafka==2.6.1   # 3
COPY *.py .                                     # 4
```

1. **`FROM`**: "empieza con una caja que ya tiene **Python 3.12** instalado". `slim` significa versión ligera.
2. **`WORKDIR`**: "trabaja dentro de la carpeta `/app`" (dentro de la caja).
3. **`RUN pip install`**: instala **`confluent-kafka`**, la librería que permite a Python hablar con Kafka. Sin ella, Python no sabría cómo conectarse.
4. **`COPY`**: copia los archivos `.py` de esta carpeta dentro de la caja.

> 💡 Si cambias un `.py`, debes **reconstruir** la caja para que se copie la versión nueva:
> `docker compose build productor && docker compose up -d productor consumidor-alertas`

---

## 4. `productor.py`: cómo funciona por dentro

### Paso A: conectarse a Kafka

```python
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
producer = Producer({"bootstrap.servers": BOOTSTRAP})
```

- `BOOTSTRAP` es la **dirección de Kafka**. La lee de una variable que viene de `docker-compose.yml`.
- **Dentro de Docker** la dirección es `kafka:9092`: las cajas se llaman entre sí por su nombre, como contactos en un teléfono.
- Si ejecutaras el programa **desde tu PC** (fuera de Docker), usaría `localhost:9094`, la puerta que Kafka abre hacia afuera.
- `Producer(...)` crea el "periodista" ya conectado.

### Paso B: inventar un pedido

```python
pedido = {
    "pedido_id": str(uuid.uuid4()),       # un código único e irrepetible
    "cliente": random.choice(CLIENTES),   # uno de 50 clientes: cliente_001 ... cliente_050
    "ciudad": random.choice(CIUDADES),    # Barranquilla, Bogotá, Medellín, Cali o Cartagena
    "producto": producto,                 # Hamburguesa, Pizza, Sushi, Arepa, Empanadas, Jugo
    "cantidad": cantidad,                 # de 1 a 4
    "precio_unitario": precio,
    "total": precio * cantidad,
    "fecha_evento": datetime.now(timezone.utc).isoformat(),   # cuándo ocurrió
}
```

Así se ve un pedido **real** cuando viaja por Kafka (formato **JSON**, que es solo texto ordenado con `{"nombre": valor}`):

```json
{
  "pedido_id": "5b86b409-dc4f-446f-99f2-a813e6e5934f",
  "cliente": "cliente_043",
  "ciudad": "Cartagena",
  "producto": "Empanadas",
  "cantidad": 4,
  "precio_unitario": 12000,
  "total": 48000,
  "fecha_evento": "2026-10-14T14:59:29.941717+00:00"
}
```

### Paso C: ensuciar algunos datos A PROPÓSITO 🧪

```python
if r < 0.02:
    pedido["total"] = -pedido["total"]   # 2%: total negativo
elif r < 0.04:
    pedido["cliente"] = None             # 2%: sin cliente
```

En la vida real **los datos siempre llegan con errores**: fallos de la app, campos vacíos, valores imposibles… Ensuciamos ~4% de los pedidos para que **Spark tenga trabajo de limpieza** y puedas ver cómo los descarta.

### Paso D: enviar el pedido a Kafka

```python
producer.produce(TOPIC, key=str(pedido["cliente"]), value=json.dumps(pedido),
                 callback=al_entregar)
```

| Parte | Significado |
|---|---|
| `TOPIC` | **A qué sección** de Kafka va: `"pedidos"` |
| `key=cliente` | La **clave**. Kafka la usa para decidir **a qué partición (carril)** va el mensaje. El mismo cliente → siempre el mismo carril → sus pedidos quedan **en orden** |
| `value=json.dumps(pedido)` | El **contenido**: el pedido convertido a texto JSON |
| `callback=al_entregar` | "Cuando Kafka confirme que lo guardó, ejecuta la función `al_entregar`" |

### Paso E: la confirmación de Kafka

```python
def al_entregar(err, msg):
    print(f"-> Pedido ... guardado en particion={msg.partition()} offset={msg.offset()}")
```

Kafka responde: *"lo guardé en el carril X, en la posición Y"*. Eso es lo que ves en pantalla:

```
-> Pedido 679b1185 | cliente_001  | Barranquilla | $    36000 guardado en particion=2 offset=2
-> Pedido 6efb0db2 | cliente_001  | Bogotá       | $    14000 guardado en particion=2 offset=3
```

🔎 Fíjate en que `cliente_001` cae **siempre en la partición 2**. Ese es el efecto de la `key`.

### Paso F: repetir para siempre

```python
while True:
    ...
    time.sleep(1 / PEDIDOS_POR_SEGUNDO)
```

Con `PEDIDOS_POR_SEGUNDO = 2`, espera medio segundo entre pedidos. **Nunca se detiene** hasta que apagues la caja.

---

## 5. `consumidor_alertas.py`: un lector independiente

```python
consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "equipo-alertas",        # ← el nombre de "su equipo"
    "auto.offset.reset": "latest",       # ← la primera vez, empieza por lo más nuevo
})
consumer.subscribe(["pedidos"])          # ← "quiero leer la sección pedidos"

while True:
    msg = consumer.poll(1.0)             # ← "¿hay algo nuevo? espero hasta 1 segundo"
    ...
```

### Conceptos clave que demuestra

**1) `group.id`: el marcapáginas propio.**
Kafka recuerda **por dónde va leyendo cada grupo**. El grupo `equipo-alertas` tiene su marcapáginas y Spark tiene el suyo. Así, **los dos leen TODOS los pedidos, sin quitárselos el uno al otro**.

> 📰 Es como un periódico: que tú lo leas no impide que tu vecino lea el mismo ejemplar.

**2) `auto.offset.reset: latest`.**
Solo aplica **la primera vez** que el grupo se conecta (cuando aún no tiene marcapáginas):
- `latest` → "empieza por los mensajes **nuevos**, ignora los viejos".
- `earliest` → "empieza **desde el principio** de la historia".

Después de la primera vez, **siempre continúa desde su marcapáginas**, aunque se apague y se vuelva a encender.

**3) `poll()`: preguntar sin parar.**
El consumidor pregunta constantemente "¿hay algo nuevo?". Si hay un pedido, lo procesa:

```
[PEDIDO GRANDE] cliente_010 en Bogotá: 4x Sushi = $208,000
[DATO SUCIO] particion=1 offset=40 -> {... 'cliente': None ...}
```

- **PEDIDO GRANDE**: total de $150.000 o más.
- **DATO SUCIO**: total negativo o cliente vacío.

> 💡 **¿Por qué existe este consumidor?** Para demostrar que **Kafka permite que muchos sistemas usen los mismos datos a la vez**. En una empresa, el equipo de fraude, el de marketing y el de logística leerían el mismo topic, cada uno para lo suyo.

---

## 6. Comandos para ver esta carpeta en acción

Ejecútalos desde la carpeta del proyecto (`cd ~/pipeline-datos`):

```bash
# Encender solo estas dos cajas (Kafka debe estar encendido antes)
docker compose up -d productor consumidor-alertas

# Ver los pedidos que se envían, en vivo       (Ctrl + C para dejar de mirar)
docker compose logs -f productor

# Ver las alertas, en vivo                       (Ctrl + C para dejar de mirar)
docker compose logs -f consumidor-alertas

# Ver por dónde va leyendo el equipo de alertas (columna LAG = cuánto le falta)
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group equipo-alertas

# Pausar / reanudar el productor (los pedidos dejan de llegar / vuelven)
docker compose stop productor
docker compose start productor
```

---

## 7. ¿Qué puedes cambiar para experimentar?

| Quiero… | Dónde | Luego ejecutar |
|---|---|---|
| Más o menos pedidos por segundo | `PEDIDOS_POR_SEGUNDO` en `../docker-compose.yml` | `docker compose up -d productor` |
| Cambiar ciudades o productos | `CIUDADES` y `PRODUCTOS` en `productor.py` | `docker compose build productor && docker compose up -d productor` |
| Más o menos datos sucios | Los números `0.02` y `0.04` en `productor.py` | igual que arriba |
| Cambiar qué es un "pedido grande" | `UMBRAL` en `consumidor_alertas.py` | `docker compose build productor && docker compose up -d consumidor-alertas` |

---

## 8. Resumen

- ✅ `productor.py` = **la fuente de datos**: inventa 2 pedidos por segundo y los envía al topic `pedidos`.
- ✅ La **key** (el cliente) decide la partición, y así los pedidos de cada cliente se mantienen en orden.
- ✅ Kafka **confirma** cada mensaje con su **partición** y su **offset**.
- ✅ `consumidor_alertas.py` demuestra que **varios lectores pueden leer lo mismo**, cada uno con su marcapáginas (`group.id`).
- ➡️ **Siguiente parada:** [`spark/`](../spark/README.md), donde estos pedidos se limpian y se guardan.
