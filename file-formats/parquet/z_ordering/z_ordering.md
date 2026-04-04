# Z-Ordering in Parquet: Multi-Dimensional Data Clustering

## Overview

Z-ordering (Morton order) is a space-filling curve technique that maps multi-dimensional data into 1D space while preserving spatial locality. Data points close to each other in multiple dimensions tend to be adjacent in the z-order sequence.

## Why Z-Ordering Matters for Parquet

### The Problem
When filtering on multiple columns simultaneously (e.g., `WHERE date >= '2023-01-01' AND amount > 5000`), standard column statistics may not be enough to skip row groups efficiently. Data could be randomly distributed, requiring many row groups to be scanned.

### The Solution
Z-order clustering physically groups data by sorting based on a space-filling curve applied to multiple columns. This ensures:
- **Better row group pruning** for multi-column range queries
- **Reduced I/O** by accessing fewer row groups
- **Improved query performance** on analytical workloads

## How It Works

1. **Normalize column values** to a common range (e.g., 0-255)
2. **Interleave bits** from each column to create a z-order coordinate
3. **Sort data** by z-order values
4. **Write to Parquet** with row groups

This ensures nearby points in multi-dimensional space stay together in the file.

## Example: Date + Amount Filtering

### Without Z-ordering (Random Distribution)
```
Row Group 1: dates=[1-10],   amounts=[100-500]     ← Must scan
Row Group 2: dates=[5-15],   amounts=[50-300]      ← Must scan
Row Group 3: dates=[20-30],  amounts=[5000+]       ← Can skip
Row Group 4: dates=[2-8],    amounts=[1000-2000]   ← Must scan
Result: 3 of 4 row groups scanned
```

### With Z-ordering (Clustered)
```
Row Group 1: dates=[1-7],    amounts=[100-500]     ← Must scan
Row Group 2: dates=[8-15],   amounts=[50-300]      ← Must scan
Row Group 3: dates=[16+],    amounts=[5000+]       ← Can skip
Row Group 4: dates=[20-30],  amounts=[1000-2000]   ← Can skip
Result: 2 of 4 row groups scanned (50% reduction!)
```

## When to Use Z-Ordering

**Best for:**
- Multi-column range queries (date ranges + numeric ranges)
- Geospatial queries (latitude, longitude, time)
- Multi-dimensional time-series data
- OLAP workloads with common filter patterns

**Not needed for:**
- Single-column filters (column statistics are sufficient)
- Queries with OR conditions
- Random access patterns


## Performance Benefits

Expected improvements with z-ordering:
- **Row group pruning**: 30-80% reduction for multi-column queries
- **Query speed**: 2-5x faster for analytical queries
- **I/O reduction**: Proportional to pruning improvement
