# File Formats Cookbook

Practical code examples for working with binary file formats in data pipelines.


## Contents

### 1. Apache Parquet: The Analytics Specialist

**Recipe 1: Hello World - Writing Parquet with PyArrow vs FastParquet**
Learn the fundamentals of converting Pandas DataFrames to Parquet format. This recipe covers the two main engines (PyArrow and FastParquet), their performance characteristics, compression options, and best practices for choosing the right engine for your use case.

**Recipe 2: Column Selection - Reading Only What You Need**
Discover the power of Parquet's columnar storage. This recipe demonstrates how to read only specific columns from wide datasets, dramatically improving read performance and reducing memory usage. Learn predicate pushdown filtering to combine column selection with row filtering for optimal efficiency.

### 2. Apache Avro: The Streaming Specialist
Recipes focused on schema evolution, serialized writing, and Kafka compatibility.

### 3. Apache ORC: The Enterprise Specialist
Recipes focused on high-compression environments and SQL-heavy workflows.

### 4. Cross-Format Bridge Recipes
Recipes that show these formats working together, including benchmarking and type mapping.

## Setup

1. **Create a Python virtual environment:**
   ```bash
   # Using venv (Python 3.6+)
   python -m venv venv
   
   # Activate the virtual environment
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
   # Navigate to the recipe directory
   cd file-formats/parquet/hello_world
   
   # Run the example
   python hello_world.py
   ```
