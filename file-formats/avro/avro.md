## Apache Avro Overview

### What is Apache Avro?
Apache Avro is an open-source, row-oriented data serialization framework designed for efficient data exchange and storage. It was created for the Hadoop ecosystem, optimized for streaming systems, write-heavy workloads, and schema-governed data pipelines.

---

### History: when was it created?
Apache Avro was proposed as a subproject of Apache Hadoop on April 2, 2009, developed by Doug Cutting, the creator of Hadoop, Lucene, and Nutch. It became an Apache top-level project on May 4, 2010.

---

### Who maintains Avro now?
- Current home: Apache Software Foundation (ASF)
- Project: [apache/avro](https://github.com/apache/avro) with language-specific APIs available in Java, Python, C++, Ruby, and others.

---

### Design principles
Apache Avro is built on several core design principles that make it well-suited for streaming and data exchange workloads. First, it uses **row-oriented storage**, storing all fields of a record contiguously. This makes it efficient for writing individual records sequentially, which is ideal for logging and streaming systems where records arrive one at a time.

The format relies on **schema-driven serialization**, where the schema is always required to read or write data. In the Object Container File format, the schema is embedded in the file itself, making it self-describing. In binary encoding, the schema is managed externally, typically through a Schema Registry, reducing message size for high-throughput systems.

Avro supports **schema evolution** through compatibility rules that allow producers and consumers to evolve their schemas independently. Fields can be added or removed over time as long as compatibility constraints are respected, making it safe to deploy schema changes without coordinating all systems simultaneously.

The format uses a **compact binary encoding** that stores only values without field names or type identifiers, resulting in smaller message sizes compared to JSON or XML. This makes it particularly efficient for network transmission and high-volume event streaming.

Finally, Avro integrates natively with **Apache Kafka and the Schema Registry**, making it the dominant serialization format in event-driven architectures where schemas must be versioned, validated, and governed centrally.

---

### Most common use cases
- Event streaming and messaging
   - Kafka producers and consumers with Schema Registry
- Data ingestion pipelines
   - Landing zone format before transformation to Parquet or ORC
- Log aggregation
   - Appending individual records to files in real time
- Schema-governed data exchange
   - Sharing data between services with guaranteed structure
- Interoperability across languages
   - `Java` ↔ `Python` ↔ `C++` ↔ `Ruby` ↔ `Go`

---

### Why Avro is popular
- Compact binary format with small message sizes
- Schema embedded in file or managed via Schema Registry
- Native support for schema evolution with compatibility rules
- Ideal for append-only and write-heavy workloads
- Strong integration with Kafka and the Hadoop ecosystem
- Supports a wide range of programming languages

For comprehensive technical details on file structure and encoding specifications, refer to the [official Apache Avro documentation](https://avro.apache.org/docs/).
