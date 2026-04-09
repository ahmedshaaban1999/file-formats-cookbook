## Apache Parquet Overview (README)

### What is Apache Parquet?

Apache Parquet is an open-source, columnar storage file format designed for fast data analytics and efficient processing of large datasets.  
It was created for the Hadoop ecosystem, optimized for data warehouses, big data processing engines, and OLAP/stores.

---

### History: when was it created?

Apache Parquet was initially released in 2013, developed by engineers at Cloudera and Twitter. The format was created to address performance and storage inefficiencies inherent in row-based formats when handling analytic workloads.

---

### Who maintains Parquet now?

- Current home: Apache Software Foundation (ASF)
- Project: (apache/parquet-format)[https://github.com/apache/parquet-format/] with language-specific APIs avaliable in Java and Rust.

---

### Design principles

### Design principles

Apache Parquet is built on several core design principles that make it exceptional for analytical workloads. First, it uses **columnar storage**, storing data organized by column rather than by row. This approach enables excellent compression ratios and efficient read pruning when queries only need specific fields, dramatically reducing I/O operations.

The format implements **schema-based metadata** where a typed schema is stored in the file footer, supporting complex nested structures like lists, maps, and structs. This enables rich data representation while maintaining type safety across the ecosystem.

**Efficient compression and encoding** is achieved through per-column encoding strategies such as dictionary encoding, run-length encoding, and bit-packing, combined with per-page and per-column statistics. These techniques are applied independently to each column, maximizing compression based on the data characteristics of that specific column.

Parquet files are organized into **row groups** and pages, making them **split-friendly for distributed processing**. This architecture allows parallel read operations by engines like Spark, Presto, and Dremio. Metadata stored in the file footer and row group indexing help query engines map byte ranges to data ranges, enabling intelligent task scheduling and data locality optimization.

Finally, Parquet supports **schema evolution with backward and forward compatibility**, allowing writing and reading systems to safely evolve schemas over time without breaking existing data pipelines. For comprehensive technical details on file structure and encoding specifications, refer to the [official Apache Parquet documentation](https://parquet.apache.org/docs/).


---

### Most common use cases

- Big data analytics / OLAP
   - ETL pipelines + ad-hoc SQL queries
- Data lake storage
   - Low-cost cloud object store (S3/ADLS/GCS)
- Machine learning feature sources 
   - Dataset ingestion in Spark, Flink, Beam
- Data warehousing
   - Clickstream, logs, metrics analytics
- Interoperability across engines
   - `Spark` ↔ `Hive` ↔ `Presto` ↔ `Trino` ↔ `DuckDB` ↔ `Pandas`

---

### Why Parquet is popular

- High compression ratio (column-specific and null/low-cardinality optimization)
- Fast column projection (read only necessary columns)
- Predicate pushdown using statistics (skip row groups)
- Strong ecosystem support (Spark, Hive, Dremio, Snowflake, BigQuery external tables)
- Mature metadata support for schema evolution
- Works well in cloud storage/distributed filesystems
