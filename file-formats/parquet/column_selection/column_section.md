# Recipe 2: Column Selection - Reading Only What You Need

## Overview

This recipe demonstrates one of Parquet's most powerful features: **columnar storage**. Unlike row-oriented formats (CSV, JSON), Parquet allows you to read only the specific columns you need, dramatically improving performance for wide datasets. Learn how to leverage this capability for faster queries and reduced memory usage.

## What You'll Learn

- How columnar storage enables selective column reading
- Performance gains from reading only necessary columns
- Memory savings compared to full file reads
- Predicate pushdown: filtering data during read
- Real-world scenarios and optimization strategies

## The Problem

Traditional formats store data row-by-row:
```
Row 1: [col1, col2, col3, ..., col100]
Row 2: [col1, col2, col3, ..., col100]
Row 3: [col1, col2, col3, ..., col100]
```

Reading any column requires processing all columns. **Total I/O = row_count × all_columns**

## The Solution: Columnar Storage

Parquet stores data column-by-column:
```
Column 1: [val1, val2, val3, ...]
Column 2: [val1, val2, val3, ...]
Column 3: [val1, val2, val3, ...]
...
Column 100: [val1, val2, val3, ...]
```

Reading specific columns only touches relevant data. **Total I/O = row_count × selected_columns**

## Key Concepts

### 1. **Selective Column Reading**
Read only the columns your analysis needs:
```python
df = pd.read_parquet(
    'file.parquet', 
    columns=['id', 'timestamp', 'value'],
    engine='pyarrow'
)
```

### 2. **Predicate Pushdown**
Filter rows during read (not after loading):
```python
filters = [('user_id', '>', 50000)]
df = pd.read_parquet(
    'file.parquet',
    columns=['id', 'user_id'],
    filters=filters,
    engine='pyarrow'
)
```

### 3. **I/O Efficiency**
Parquet file structure stores metadata about each column, enabling:
- Skipping entire row groups
- Processing only relevant columns
- Applying filters without full scan


## Advanced Tips

1. **Query Planning**: Identify essential columns early in your analysis
2. **Predicate Efficiency**: Filter on low-cardinality columns (dates, categories)
3. **Combined Approach**: Use column selection + filtering together
4. **Statistics**: Parquet stores min/max stats enabling efficient filtering
5. **Benchmarking**: Profile your specific workload for optimization
