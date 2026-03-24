import json
import fastavro

# Load schema from a .avsc file
with open("order.avsc") as f:
    schema_from_file = fastavro.parse_schema(json.load(f))

print("Schema loaded from file:")
print(schema_from_file)

# Define schema inline as a Python dictionary
inline_schema = fastavro.parse_schema({
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

print("\nSchema defined inline:")
print(inline_schema)