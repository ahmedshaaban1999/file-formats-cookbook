# Recipe 1: Hello World - Writing Parquet with PyArrow vs FastParquet

## Overview

You'll learn the fundamentals of converting a Pandas DataFrame to Parquet format using two popular engines: **PyArrow** and **FastParquet**. This recipe demonstrates the basic workflow and helps you understand the performance characteristics of each engine.

## What You'll Learn

- How to convert a Pandas DataFrame to Parquet format
- Differences between PyArrow and FastParquet engines
- File size considerations with compression
- Read/write performance metrics
- Best practices for choosing an engine

## Concepts

### PyArrow
- **Pros**: Industry standard, better performance, most maintained, supports complex types
- **Cons**: Larger dependency footprint
- **Best for**: Production systems, complex schemas, performance-critical applications

### FastParquet
- **Pros**: Lightweight, sometimes faster for specific workloads
- **Cons**: Less comprehensive feature support, less actively maintained
- **Best for**: Simple use cases, resource-constrained environments

## Key Topics

### Compression
Both engines support multiple compression algorithms:
- `snappy`: Fast compression, good balance between speed and size (default)
- `gzip`: Better compression ratio, slower
- `lz4`: Very fast, less compression
- `zstd`: Modern, excellent compression ratio and speed

### Engine Selection
The choice between PyArrow and FastParquet depends on:
1. **Performance requirements**: PyArrow generally faster for I/O
2. **Data complexity**: PyArrow handles complex types better
3. **Dependencies**: FastParquet is lighter weight
4. **Ecosystem integration**: PyArrow integrates better with Arrow ecosystem


## Advanced Tips

1. **Profile your data**: Different data types compress differently. Text/strings compress well, floats less so.

2. **Choose compression wisely**: 
   - For I/O bound systems: use `snappy` or `lz4`
   - For storage-bound: use `gzip` or `zstd`

3. **Benchmark in your environment**: Performance varies by hardware, data characteristics, and system load.

4. **Engine consistency**: Use the same engine for reading and writing to avoid compatibility issues.

