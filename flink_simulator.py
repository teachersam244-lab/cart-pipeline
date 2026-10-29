from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'raw-carts',
    bootstrap_servers=['localhost:19092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers=['localhost:19092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for mensaje in consumer:
    carrito = mensaje.value

    try:
        if "id" not in carrito:
            raise ValueError("Falta la clave 'id'")

        for clave in ["total", "discountedTotal", "totalProducts"]:
            if clave not in carrito:
                raise ValueError(f"Falta la clave '{clave}'")

        if carrito["totalProducts"] == 0:
            raise ValueError("totalProducts es 0")

        carrito["discountAmount"] = carrito["total"] - carrito["discountedTotal"]
        carrito["discountPercentage"] = (carrito["discountAmount"] / carrito["total"]) * 100
        carrito["averageProductValue"] = carrito["total"] / carrito["totalProducts"]
        carrito["status"] = "PROCESSED"

        producer.send('processed-carts', carrito)
        producer.flush()
        print(f"Procesado carrito id={carrito['id']}")

    except (ValueError, KeyError, TypeError, ZeroDivisionError) as e:
        error_msg = {
            "original": carrito,
            "error": str(e),
            "type": type(e).__name__
        }
        producer.send('errors-carts', error_msg)
        producer.flush()
        print(f"Carrito rechazado: {e}")
        continue

        