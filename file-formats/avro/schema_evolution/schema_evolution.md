# Schema Evolution

## Overview
Schema Evolution refers to the ability to change a data schema over time without breaking existing systems. It ensures that producers and consumers can continue to operate even as the structure of the data evolves.

In Avro, schema evolution is achieved through compatibility rules that define how new schema versions interact with existing data.

## What You'll Learn
- What schema evolution is and why it matters
- The difference between backward and forward compatibility
- Common schema changes and their impact
- How Avro handles schema evolution at read time

## Concepts

### Schema Versioning
Schemas evolve through versions. Each new version introduces changes that must remain compatible with previous versions depending on the chosen compatibility mode.

### Backward Compatibility
An old schema can read data written with a newer schema. In this recipe, V1 reads data written by V2:
```python
with open("orders_v2.avro", "rb") as f:
    reader = fastavro.reader(f, reader_schema=parsed_v1)
```

### Forward Compatibility
A new schema can read data written with an older schema. In this recipe, V2 reads data written by V1, and fills the missing `country` field with its default value:
```python
with open("orders_v1.avro", "rb") as f:
    reader = fastavro.reader(f, reader_schema=parsed_v2)
```

### Full Compatibility
A schema is both backward and forward compatible. V2 in this recipe achieves full compatibility because it only adds a field with a default value, allowing both old and new readers to handle each other's data safely.

### Compatibility Summary

| Allowed Change | Backward | Forward | Full |
|---|---|---|---|
| Add optional field | ✓ | ✓ | ✓ |
| Remove optional field | ✓ | ✓ | ✓ |
| Add required field | | ✓ | |
| Remove required field | ✓ | | |

### Reader Schema vs Writer Schema
Avro resolves differences between schemas at read time:
- **Writer Schema**: used when data is written
- **Reader Schema**: used when data is read

Avro applies resolution rules to map fields between them.

## Key Topics

### Adding a Field
Adding a new field is safe only if a default value is provided. Without a default value, old data cannot be read by the new schema:
```python
{"name": "country", "type": "string", "default": "Egypt"}
```

### Removing a Field
Removing a field is safe only if it was optional or not required by consumers. In this recipe, V3 removes `status` and reads V1 data without errors:
```python
with open("orders_v1.avro", "rb") as f:
    reader = fastavro.reader(f, reader_schema=parsed_v3)
```

### Changing a Field Type
Changing a field type is generally not compatible. For example, changing `price` from `double` to `string` will break serialization and deserialization.

### Optional Fields in Avro
Avro does not have a built-in optional keyword. Instead, optional fields are defined using union types:
```python
{"name": "status", "type": ["null", "string"], "default": None}
```

## Advanced Tips
- Always add new fields with default values
- Avoid changing field types in existing schemas
- Use optional fields when future changes are expected
- Validate schema changes before deploying
- Treat schema changes as part of system design, not just data structure updates
