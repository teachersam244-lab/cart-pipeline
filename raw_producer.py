import requests
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

response = requests.get("https://dummyjson.com/carts")
data = response.json()
carritos = data["carts"]

contador = 0
for carrito in carritos:
    producer.send('raw-carts', carrito)
    contador += 1

producer.flush()
print(f"Publicados {contador} carritos en raw-carts")