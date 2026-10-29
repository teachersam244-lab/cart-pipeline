import requests

response= requests.get("https://dummyjson.com/carts")

if response.status_code ==200:

    data = response.json()

    carrito = data["carts"][0]

    print("id:", carrito["id"])

    print("userId:", carrito["userId"])

    print("total:", carrito["total"])

    print("discountedTotal:", carrito["discountedTotal"])

    print("totalProducts:", carrito["totalProducts"])

    print("totalQuantity", carrito["totalQuantity"])

    print("Número de productos en el carrito:", len(carrito["products"]))

