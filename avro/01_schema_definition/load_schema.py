import json
import fastavro

with open("order.avsc") as f:
    schema = fastavro.parse_schema(json.load(f))

print(schema)