# Serialized Writing

## Overview
Avro supports three serialization formats, each suited for different use cases. The choice of format depends on whether the schema needs to travel with the data, how much storage efficiency matters, and whether human readability is required.

## What You'll Learn
- The three Avro serialization formats and their differences
- When to use each format based on your use case
- How to write and read records using each format

## Concepts

### Object Container File
The schema is stored inside the file alongside the data. This is the standard format for batch processing and long-term storage, as the file is self-describing and can be read without any external schema. Records are stored in compressed blocks, making the format efficient for large datasets and compatible with distributed processing frameworks like Spark and Hadoop.

### Binary Encoding
Records are serialized to raw bytes without embedding the schema. This produces the smallest possible messages, making it the preferred format for streaming systems like Kafka where the schema is managed externally by a Schema Registry. The schema must be available at read time, as the binary data alone is not self-describing.

### JSON Encoding
Records are serialized to human-readable JSON. This format is useful for debugging and interoperability with systems that consume JSON natively. It is not suitable for production use due to its larger size and slower parsing compared to binary formats.

## Key Topics

### Appending Records
Unlike Parquet, which requires all data to be available before writing, Avro allows appending individual records to an existing file without rewriting it. This makes it ideal for logging and streaming systems where new records arrive continuously.

### Schema Availability
Object Container Files are self-describing. Binary encoded messages require the schema to be supplied externally at read time, which is typically handled by a Schema Registry in production systems.

## Advanced Tips
- Use Object Container Files for batch jobs and data lake storage
- Use Binary Encoding for Kafka messages and low-latency streaming pipelines
- Use JSON Encoding only for debugging, never in production
- When using Binary Encoding, always pair it with a Schema Registry to manage schema versions
