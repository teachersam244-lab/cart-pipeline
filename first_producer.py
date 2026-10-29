from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
producer.send('raw-carts', {'message': 'hello pipeline'})
producer.flush()
print("Mensaje enviado")

