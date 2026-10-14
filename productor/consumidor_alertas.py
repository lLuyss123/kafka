"""
CONSUMIDOR DE ALERTAS: otro "equipo" de la empresa que lee el MISMO topic.
Tiene su propio group.id, así que lleva su propio marcapáginas (offset)
y no le quita mensajes a Spark ni a nadie más.
"""
import json
import os

from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
UMBRAL = 150000

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "equipo-alertas",
    "auto.offset.reset": "latest",   # solo pedidos nuevos desde que arranca
})
consumer.subscribe(["pedidos"])
print(f"Equipo de alertas escuchando pedidos mayores a ${UMBRAL:,}...")

while True:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    pedido = json.loads(msg.value())
    if pedido["total"] < 0 or pedido["cliente"] is None:
        print(f"[DATO SUCIO] particion={msg.partition()} offset={msg.offset()} -> {pedido}")
    elif pedido["total"] >= UMBRAL:
        print(f"[PEDIDO GRANDE] {pedido['cliente']} en {pedido['ciudad']}: "
              f"{pedido['cantidad']}x {pedido['producto']} = ${pedido['total']:,}")
