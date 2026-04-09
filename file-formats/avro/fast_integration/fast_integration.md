# Fast Integration

## Overview
Avro data is not always read from files on disk. In streaming systems like Kafka, Avro records arrive as raw bytes in memory. This recipe demonstrates how to serialize and deserialize Avro records directly in memory without writing to disk, which is the standard pattern in event-driven architectures.

## What You'll Learn
- How to serialize Avro records to bytes in memory
- How to deserialize Avro bytes directly into Python dictionaries
- Why this pattern is used in streaming systems like Kafka

## Concepts

### In-Memory Serialization
Instead of writing to a file, records are serialized into a `BytesIO` buffer in memory:

```python
buffer = io.BytesIO()
fastavro.writer(buffer, schema, orders)
avro_bytes = buffer.getvalue()
```

### In-Memory Deserialization
The raw bytes are read back directly into Python dictionaries without touching the disk:

```python
buffer = io.BytesIO(avro_bytes)
records = list(fastavro.reader(buffer))
```

## Key Topics

### Why In-Memory?
In file-based workflows, Avro records are written to and read from `.avro` files. In streaming workflows, records travel as bytes over a network. When a Kafka consumer receives a message, it receives raw bytes, not a file. Reading those bytes directly into memory is faster and avoids unnecessary disk I/O.

### BytesIO
`io.BytesIO` is a Python standard library class that behaves like a file but operates entirely in memory. fastavro works with any file-like object, so `BytesIO` allows the same read and write API to be used without a real file.

## Advanced Tips
- Use this pattern when consuming Avro messages from Kafka
- Combine with Binary Encoding for even smaller message sizes
- Always keep the schema available when using in-memory deserialization, as the bytes alone are not self-describing without an Object Container header