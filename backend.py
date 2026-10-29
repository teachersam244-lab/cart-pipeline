from fastapi import FastAPI
from kafka import KafkaConsumer
import threading
import json

app = FastAPI()
from fastapi.middleware.cors import CORSMiddleware


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Almacen en memoria
carts_cache = []
errors_cache = []

def consume_kafka():
    """Consumidor que corre en segundo plano."""
    consumer = KafkaConsumer(
        'processed-carts',
        'errors-carts',
        bootstrap_servers=['localhost:19092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    
    for mensaje in consumer:
        if mensaje.topic == 'processed-carts':
                carts_cache.append(mensaje.value)
                print(f"Carrito almacenado: id={mensaje.value.get('id')}")
        elif mensaje.topic == 'errors-carts':
                errors_cache.append(mensaje.value)
                print(f"Error almacenado: {mensaje.value.get('error')}")

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=consume_kafka, daemon=True)
    thread.start()
    print("Consumidor de Kafka iniciado en segundo plano")

@app.get("/health")
def health():
    return {"status": "UP"}

@app.get("/api/carts")
def get_carts():
    return {"carts": carts_cache, "total": len(carts_cache)}
    
@app.get("/api/metrics")
def get_metrics():
    total_carts = len(carts_cache)
    total_value = sum(c["total"] for c in carts_cache if "total" in c)
    total_discount = sum(c["discountAmount"] for c in carts_cache if "discountAmount" in c)
    average_value = total_value / total_carts if total_carts > 0 else 0
    total_errors = len(errors_cache)
    last_cart = carts_cache[-1] if carts_cache else None
    
    return {
        "total_carts": total_carts,
        "total_value": total_value,
        "total_discount": total_discount,
        "average_value": average_value,
        "total_errors": total_errors,
        "last_cart_id": last_cart["id"] if last_cart else None
    }
@app.get("/api/status")
def get_status():
    return {
        "backend": "UP",
        "kafka_connected": True,
        "carts_cached": len(carts_cache),
        "errors_cached": len(errors_cache)
    }
if len(carts_cache) % 10 == 0:
    print(f"Procesados {len(carts_cache)} carritos...")