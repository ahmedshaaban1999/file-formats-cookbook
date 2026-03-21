import json
import fastavro
from pathlib import Path

SCHEMA_PATH = "../01_schema_definition/order.avsc"
OUTPUT_FILE = "orders.avro"

with open(SCHEMA_PATH) as f:
    schema = fastavro.parse_schema(json.load(f))

# Write initial orders to a new Avro file
orders = [
    {"order_id": 1, "customer_name": "Ahmed", "product": "Laptop", "quantity": 1, "price": 999.99, "status": "pending"},
    {"order_id": 2, "customer_name": "Sara", "product": "Phone", "quantity": 2, "price": 499.99, "status": "shipped"},
    {"order_id": 3, "customer_name": "Omar", "product": "Tablet", "quantity": 1, "price": 299.99, "status": None},
]

with open(OUTPUT_FILE, "wb") as f:
    fastavro.writer(f, schema, orders)

print(f"Written {len(orders)} orders to {OUTPUT_FILE}")

# Append a new order without rewriting the file
new_order = {"order_id": 4, "customer_name": "Mona", "product": "Headphones", "quantity": 3, "price": 149.99, "status": "pending"}

with open(OUTPUT_FILE, "a+b") as f:
    fastavro.writer(f, schema, [new_order])

print(f"Appended 1 order to {OUTPUT_FILE}")

# Read all orders back from the file
with open(OUTPUT_FILE, "rb") as f:
    for order in fastavro.reader(f):
        print(order)