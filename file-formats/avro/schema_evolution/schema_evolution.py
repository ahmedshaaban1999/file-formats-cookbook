import json
import fastavro

# Schema V1: base schema
schema_v1 = {
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
}

# Schema V2: add a new field with a default value (forward compatible)
schema_v2 = {
    "type": "record",
    "name": "Order",
    "namespace": "com.cookbook",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "customer_name", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "price", "type": "double"},
        {"name": "status", "type": ["null", "string"], "default": None},
        {"name": "country", "type": "string", "default": "Egypt"}
    ]
}

# Schema V3: remove a field (backward compatible)
schema_v3 = {
    "type": "record",
    "name": "Order",
    "namespace": "com.cookbook",
    "fields": [
        {"name": "order_id", "type": "int"},
        {"name": "customer_name", "type": "string"},
        {"name": "product", "type": "string"},
        {"name": "quantity", "type": "int"},
        {"name": "price", "type": "double"}
    ]
}

parsed_v1 = fastavro.parse_schema(schema_v1)
parsed_v2 = fastavro.parse_schema(schema_v2)
parsed_v3 = fastavro.parse_schema(schema_v3)

# Sample data written with V1
orders_v1 = [
    {"order_id": 1, "customer_name": "Ahmed", "product": "Laptop", "quantity": 1, "price": 999.99, "status": "pending"},
    {"order_id": 2, "customer_name": "Sara", "product": "Phone", "quantity": 2, "price": 499.99, "status": "shipped"},
    {"order_id": 3, "customer_name": "Omar", "product": "Tablet", "quantity": 1, "price": 299.99, "status": None},
]

# Write using V1
with open("orders_v1.avro", "wb") as f:
    fastavro.writer(f, parsed_v1, orders_v1)

print("Data written using V1\n")

# Forward compatibility: new reader (V2) reads old data (V1)
with open("orders_v1.avro", "rb") as f:
    reader = fastavro.reader(f, reader_schema=parsed_v2)
    forward_records = list(reader)

print("Forward compatibility (V2 reads V1 data):")
for r in forward_records:
    print(r)

# Write new data using V2
orders_v2 = [
    {"order_id": 4, "customer_name": "Mona", "product": "Headphones", "quantity": 1, "price": 149.99, "status": "delivered", "country": "Egypt"}
]

with open("orders_v2.avro", "wb") as f:
    fastavro.writer(f, parsed_v2, orders_v2)

# Backward compatibility: old reader (V1) reads new data (V2)
with open("orders_v2.avro", "rb") as f:
    reader = fastavro.reader(f, reader_schema=parsed_v1)
    backward_records = list(reader)

print("\nBackward compatibility (V1 reads V2 data):")
for r in backward_records:
    print(r)

# Backward compatibility: V3 (removed fields) reads V1 data
with open("orders_v1.avro", "rb") as f:
    reader = fastavro.reader(f, reader_schema=parsed_v3)
    v3_records = list(reader)

print("\nBackward compatibility (V3 reads V1 data, removed fields ignored):")
for r in v3_records:
    print(r)