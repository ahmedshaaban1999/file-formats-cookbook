import json
import io
import fastavro

SCHEMA_PATH = "../schema_definition/order.avsc"
with open(SCHEMA_PATH) as f:
    schema = fastavro.parse_schema(json.load(f))

orders = [
    {"order_id": 1, "customer_name": "Ahmed", "product": "Laptop", "quantity": 1, "price": 999.99, "status": "pending"},
    {"order_id": 2, "customer_name": "Sara", "product": "Phone", "quantity": 2, "price": 499.99, "status": "shipped"},
    {"order_id": 3, "customer_name": "Omar", "product": "Tablet", "quantity": 1, "price": 299.99, "status": None},
]

# 1. Object Container File: schema is stored inside the file alongside the data
with open("orders.avro", "wb") as f:
    fastavro.writer(f, schema, orders)

with open("orders.avro", "rb") as f:
    container_records = list(fastavro.reader(f))

print("Object Container File:")
for record in container_records:
    print(record)

# 2. Binary Encoding: serialize each record to raw bytes without embedding the schema
buffer = io.BytesIO()
for order in orders:
    fastavro.schemaless_writer(buffer, schema, order)

buffer.seek(0)
binary_records = []
for _ in orders:
    binary_records.append(fastavro.schemaless_reader(buffer, schema))

print("\nBinary Encoding:")
for record in binary_records:
    print(record)

# 3. JSON Encoding: serialize records to JSON for debugging and human readability
print("\nJSON Encoding:")
for order in orders:
    print(json.dumps(order, indent=2))