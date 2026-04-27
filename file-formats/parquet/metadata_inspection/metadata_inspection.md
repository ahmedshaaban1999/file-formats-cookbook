# Recipe 3: Metadata Inspection - Exploring Files Without Reading Data

## Overview

One of Parquet's most powerful features is the richness of its metadata. This recipe teaches you how to inspect file statistics, schema information, and row group metadata **without reading any data**. Learn to answer critical questions about your dataset instantly, enabling smart query planning and optimization.

## What You'll Learn

- How to read Parquet metadata without loading data
- Extracting schema information programmatically
- Understanding row group statistics
- Predicate pushdown opportunities from min/max values
- Estimating memory requirements before queries
- Real-world use cases for metadata inspection

## The Problem

Traditional workflows:
```
Question: How many records are in this file?
Traditional: Load entire file into memory
Parquet: Read metadata (instant)

Question: What column types do I have?
Traditional: Load data and check dtypes
Parquet: Read schema metadata (instant)

Question: Will my query need 5GB or 50GB?
Traditional: Try it and see
Parquet: Estimate from column statistics
```

## Parquet Metadata Structure

```
Parquet File
├── Data Section
│   ├── Row Group 0
│   │   ├── Column 0 (chunks)
│   │   ├── Column 1 (chunks)
│   │   └── ...
│   ├── Row Group 1
│   │   └── ...
│   └── ...
└── Metadata Section (Footer)
    ├── File Metadata
    │   ├── num_rows
    │   ├── num_row_groups
    │   └── schema
    ├── Row Group 0 Metadata
    │   ├── num_rows
    │   ├── Column 0 Stats
    │   │   ├── min_value
    │   │   ├── max_value
    │   │   ├── null_count
    │   │   └── ...
    │   └── ...
    └── Row Group N Metadata
```

**Key insight**: Footer is read last, enabling quick metadata access!

## Key Concepts

### 1. **File-Level Metadata**
Fast access to:
- Total row count
- Number of row groups
- File compression codec
- Total file size

```python
import pyarrow.parquet as pq

pf = pq.ParquetFile('data.parquet')
metadata = pf.metadata

print(f"Rows: {metadata.num_rows}")
print(f"Row groups: {metadata.num_row_groups}")
print(f"Size: {os.path.getsize('data.parquet')} bytes")
```

### 2. **Row Group Statistics**
Each row group stores:
- Min/max values per column
- Null counts
- Compression statistics

**Enables**: Skip entire row groups without reading

```python
for rg_idx in range(metadata.num_row_groups):
    rg = metadata.row_group(rg_idx)
    print(f"Row Group {rg_idx}: {rg.num_rows} rows")
    # Use statistics for query planning
```

### 3. **Schema Information**
Read column names and types instantly:
- Data types (int, float, string, etc.)
- Nullability
- Complex types (lists, structs)

```python
schema = pf.schema
for col in schema:
    print(f"{col.name}: {col.type}")
```

### 4. **Predicate Pushdown Planning**
Use min/max statistics to determine if filtering can skip row groups:

```python
# Query: amount > 5000
# If row_group.max(amount) < 5000:
#    SKIP this entire row group (no I/O needed!)
```

## Running the Recipe

```bash
python metadata_inspection.py
```

This will:
1. Create a sample dataset (100k transactions)
2. Write to Parquet file
3. Inspect file metadata
4. Show row group statistics
5. Demonstrate predicate pushdown
6. Estimate memory requirements


## Metadata Statistics Reference

| Statistic | Meaning | Use Case |
|-----------|---------|----------|
| `num_rows` | Total records | Query planning |
| `num_row_groups` | Parallelism units | Thread allocation |
| `min` | Smallest value | Filtering decisions |
| `max` | Largest value | Range queries |
| `null_count` | Missing values | Data quality |
| `total_compressed_size` | Data bytes | Network/I/O estimates |
| `total_uncompressed_size` | Memory needed | Cache sizing |


## Common Use Cases and Code

### Quick Row Count
```python
row_count = pq.ParquetFile('file.parquet').metadata.num_rows
print(f"Rows: {row_count}")
```

### Dataset Size Assessment
```python
pf = pq.ParquetFile('file.parquet')
sizes = []
for rg_idx in range(pf.metadata.num_row_groups):
    rg = pf.metadata.row_group(rg_idx)
    sizes.append(rg.total_byte_size)

print(f"Avg row group size: {np.mean(sizes) / (1024**2):.2f} MB")
```

### Date Range Detection
```python
pf = pq.ParquetFile('events.parquet')
timestamp_idx = pf.schema.get_field_index('timestamp')

min_date = None
max_date = None

for rg_idx in range(pf.metadata.num_row_groups):
    col = pf.metadata.row_group(rg_idx).column(timestamp_idx)
    if col.is_stats_set:
        stats = col.statistics
        if min_date is None or stats.min < min_date:
            min_date = stats.min
        if max_date is None or stats.max > max_date:
            max_date = stats.max

print(f"Data spans: {min_date} to {max_date}")
```

### Column Presence Check
```python
# Safely check if column exists
pf = pq.ParquetFile('file.parquet')
if 'new_column' in [pf.schema.column(i).name for i in range(len(pf.schema))]:
    print("Column found!")
else:
    print("Column missing (schema evolution not applied?)")
```


## Advanced Tips

1. **Cache metadata in production**: Parse once, reuse results
2. **Use in query optimizers**: Route queries based on statistics
3. **Monitor row group skew**: Uneven sizes indicate repartitioning need
4. **Track statistics quality**: Invalid stats break query optimization
5. **Combine with partitioning**: Double-check directories + file stats


## Limitations

⚠️ **Statistics may not be available**:
- Older Parquet files
- Different engines (some skip stats)
- Complex data types
- Solution: Check `is_stats_set` before using

⚠️ **Statistics can be stale**:
- Only captured at write time
- Don't reflect post-write data changes
- Solution: Trust source, validate if modified
