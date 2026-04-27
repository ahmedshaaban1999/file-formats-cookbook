# File Formats Cookbook

Practical code examples for working with binary file formats in data pipelines.

## Contents

### 1. Apache Parquet: The Analytics Specialist

**Recipe 1: Hello World - Writing Parquet with PyArrow vs FastParquet**
Learn the fundamentals of converting Pandas DataFrames to Parquet format. This recipe covers the two main engines (PyArrow and FastParquet), their performance characteristics, compression options, and best practices for choosing the right engine for your use case.

**Recipe 2: Column Selection - Reading Only What You Need**
Discover the power of Parquet's columnar storage. This recipe demonstrates how to read only specific columns from wide datasets, dramatically improving read performance and reducing memory usage. Learn predicate pushdown filtering to combine column selection with row filtering for optimal efficiency.

**Recipe 3: Metadata Inspection - Exploring Files Without Reading Data**
Learn how to inspect Parquet file metadata, schema information, and row group statistics without loading any actual data. This recipe shows you how to extract critical information like record counts, column types, and min/max values for smart query planning and optimization.

**Recipe 4: Partitioning - Organizing Data for Query Optimization**
Learn how to organize Parquet files into directory hierarchies based on date ranges and other dimensions. This recipe demonstrates time-based partitioning strategies, how to dramatically improve query performance by enabling partition pruning, and best practices for choosing partition keys that match your query patterns.

**Recipe 5: Schema Evolution - Adding Columns Without Breaking Readers**
Master schema evolution techniques to add new columns to existing datasets without breaking old readers. This recipe demonstrates backward and forward compatibility, merging Version 1 and Version 2 files using the V2 schema, handling type conversions for date fields, and best practices for gradual schema migrations with safe default values.

**Recipe 6: Z-Ordering - Optimizing Multi-Dimensional Query Performance**
Learn Z-ordering (also known as Morton order) to optimize query performance across multiple columns. This recipe demonstrates how Z-ordered clustering arranges data to improve cache locality and query speed when filtering on multiple dimensions, comparing performance with standard row-major ordering.

### 2. Apache Avro: The Streaming Specialist

**Recipe 1: Schema Definition - Defining Data Contracts in Avro**
Learn how to define Avro schemas using `.avsc` files and inline Python dictionaries. This recipe covers the schema structure, field types, optional fields, and the difference between file-based and inline schema definitions.

**Recipe 2: Serialized Writing - Writing and Appending Records**
Explore the three Avro serialization formats: Object Container File, Binary Encoding, and JSON Encoding. This recipe demonstrates how to write and append individual records to Avro files and explains when to use each format.

**Recipe 3: Schema Evolution - Evolving Schemas Without Breaking Consumers**
Master Avro's schema evolution capabilities. This recipe demonstrates backward compatibility, forward compatibility, and full compatibility through practical examples of adding fields, removing fields, and understanding how Avro resolves differences between writer and reader schemas at read time.

**Recipe 4: Compression Toggling - Choosing the Right Codec**
Learn how Avro supports multiple compression codecs inside Object Container Files. This recipe compares null, deflate, snappy, and zstandard codecs across 10,000 records, showing real file size differences and helping you choose the right codec based on your speed and storage requirements.

**Recipe 5: Fast Integration - Reading Avro Bytes Directly from Memory**
Learn how to serialize and deserialize Avro records directly in memory without writing to disk. This recipe demonstrates the standard pattern used in streaming systems like Kafka, where Avro records arrive as raw bytes and must be read efficiently without intermediate file I/O.

### 3. Apache ORC: The Enterprise Specialist

Recipes focused on high-compression environments and SQL-heavy workflows.

### 4. Cross-Format Bridge Recipes

Recipes that show these formats working together, including benchmarking and type mapping.

## Setup

1. **Create a Python virtual environment:**
```bash
   python -m venv venv

   # On Windows:
   venv\Scripts\activate
   # On macOS/Linux:
   source venv/bin/activate
```

2. **Install dependencies from requirements.txt:**
```bash
   pip install -r requirements.txt
```

3. **Run a recipe:**
```bash
   cd file-formats/avro/schema_definition
   python schema_definition.py
```
