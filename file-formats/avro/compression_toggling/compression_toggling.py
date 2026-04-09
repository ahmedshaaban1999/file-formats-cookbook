import io
import random
import fastavro

# Schema defined inline
schema = fastavro.parse_schema({
    "type": "record",
    "name": "Order",
    "namespace": "com.cookbook",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "customer_name", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "price", "type": "double"},
        {"name": "status", "type": ["null", "string"], "default": None}
    ]
})

# Generate 10000 orders to make compression differences visible
names = ["Ahmed", "Sara", "Omar", "Mona", "Ali", "Nour", "Hassan", "Dina"]
products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor", "Keyboard"]
statuses = ["pending", "shipped", "delivered", None]

orders = [
    {
        "order_id": i,
        "customer_name": random.choice(names),
        "product": random.choice(products),
        "quantity": random.randint(1, 10),
        "price": round(random.uniform(50, 2000), 2),
        "status": random.choice(statuses)
    }
    for i in range(10000)
]


# Avro compression codecs: null, deflate, snappy, zstandard
codecs = ["null", "deflate", "snappy", "zstandard"]

for codec in codecs:
    filename = f"orders_{codec}.avro"

    # Write with the specified codec
    with open(filename, "wb") as f:
        fastavro.writer(f, schema, orders, codec=codec)

    # Read back and measure file size
    with open(filename, "rb") as f:
        size = len(f.read())

    print(f"Codec: {codec:<12} Size: {size} bytes")