"""
PRODUCTOR: simula la app de domicilios.
Cada pedido que hace un cliente se publica como un EVENTO en el topic "pedidos" de Kafka.
A propósito, ~5% de los pedidos salen "sucios" para que Spark tenga algo que limpiar.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC = "pedidos"
PEDIDOS_POR_SEGUNDO = float(os.getenv("PEDIDOS_POR_SEGUNDO", "2"))

CIUDADES = ["Barranquilla", "Bogotá", "Medellín", "Cali", "Cartagena"]
PRODUCTOS = {"Hamburguesa": 25000, "Pizza": 38000, "Sushi": 52000,
             "Arepa": 9000, "Empanadas": 12000, "Jugo": 7000}
CLIENTES = [f"cliente_{n:03d}" for n in range(1, 51)]

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def al_entregar(err, msg):
    """Kafka nos confirma dónde guardó el mensaje: partición y offset (el 'marcapáginas')."""
    if err:
        print(f"ERROR al enviar: {err}")
        return
    pedido = json.loads(msg.value())
    print(f"-> Pedido {pedido['pedido_id'][:8]} | {str(pedido['cliente']):12} | "
          f"{pedido['ciudad']:12} | ${pedido['total']:>9} "
          f"guardado en particion={msg.partition()} offset={msg.offset()}")


def crear_pedido():
    producto, precio = random.choice(list(PRODUCTOS.items()))
    cantidad = random.randint(1, 4)
    pedido = {
        "pedido_id": str(uuid.uuid4()),
        "cliente": random.choice(CLIENTES),
        "ciudad": random.choice(CIUDADES),
        "producto": producto,
        "cantidad": cantidad,
        "precio_unitario": precio,
        "total": precio * cantidad,
        "fecha_evento": datetime.now(timezone.utc).isoformat(),
    }
    # Datos sucios intencionales (como en la vida real)
    r = random.random()
    if r < 0.02:
        pedido["total"] = -pedido["total"]   # total negativo (error de la app)
    elif r < 0.04:
        pedido["cliente"] = None             # cliente vacío
    return pedido


if __name__ == "__main__":
    print(f"Productor conectado a {BOOTSTRAP}. Enviando {PEDIDOS_POR_SEGUNDO} pedidos/seg al topic '{TOPIC}'...")
    while True:
        pedido = crear_pedido()
        # La KEY decide la partición: todos los pedidos de un mismo cliente caen
        # en la misma partición y por eso se mantienen en orden.
        producer.produce(TOPIC, key=str(pedido["cliente"]), value=json.dumps(pedido),
                         callback=al_entregar)
        producer.poll(0)          # dispara los callbacks de confirmación
        time.sleep(1 / PEDIDOS_POR_SEGUNDO)
