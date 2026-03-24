# Serialized Writing

Avro supports three serialization formats, each suited for different use cases. The choice of format depends on whether the schema needs to travel with the data, how much storage efficiency matters, and whether human readability is required.

## Object Container File

The schema is stored inside the file alongside the data. This is the standard format for batch processing and long-term storage, as the file is self-describing and can be read without any external schema. Records are stored in compressed blocks, making the format efficient for large datasets and compatible with distributed processing frameworks like Spark and Hadoop.

## Binary Encoding

Records are serialized to raw bytes without embedding the schema. This produces the smallest possible messages, making it the preferred format for streaming systems like Kafka where the schema is managed externally by a Schema Registry. The downside is that the schema must be available at read time, as the binary data alone is not self-describing.

## JSON Encoding

Records are serialized to human-readable JSON. This format is useful for debugging and interoperability with systems that consume JSON natively. It is not suitable for production use due to its larger size and slower parsing compared to binary formats.