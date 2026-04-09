import io
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

orders = [
    {"order_id": 1, "customer_name": "Ahmed", "product": "Laptop", "quantity": 1, "price": 999.99, "status": "pending"},
    {"order_id": 2, "customer_name": "Sara", "product": "Phone", "quantity": 2, "price": 499.99, "status": "shipped"},
    {"order_id": 3, "customer_name": "Omar", "product": "Tablet", "quantity": 1, "price": 299.99, "status": None},
]

# Serialize records to bytes in memory without writing to disk
buffer = io.BytesIO()
fastavro.writer(buffer, schema, orders)
avro_bytes = buffer.getvalue()

print(f"Serialized {len(orders)} records to {len(avro_bytes)} bytes in memory")

# Read Avro bytes directly from memory into Python dictionaries
buffer = io.BytesIO(avro_bytes)
records = list(fastavro.reader(buffer))

print("\nRecords read from memory:")
for record in records:
    print(record)