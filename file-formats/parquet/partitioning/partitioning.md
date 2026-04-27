# Recipe 4: Partitioning - Organizing Data for Query Optimization

## Overview

Partitioning is a critical technique for managing large datasets efficiently. This recipe shows how to organize Parquet files into directory hierarchies based on date ranges (year/month/day), enabling the system to skip entire directories during queries. Learn how partitioning dramatically improves query performance on time-series data.

## What You'll Learn

- How to partition Parquet data by time-based hierarchies
- Performance gains from selective directory scanning
- Directory structure best practices
- Trade-offs between partition granularity and query patterns
- Real-world partition strategies for big data systems


## Key Concepts

### Partition Scheme Selection

**By Date (Most Common)**
```
year=YYYY/month=MM/day=DD/
```
- Best for: Time-series data, event logs, metrics
- Granularity options: year, month, day, hour
- Query patterns: historic data retrieval, time ranges

**By Category**
```
region=US/country=CA/
```
- Best for: Geographic or organizational data
- Efficient for: Regional analytics, multi-tenant systems

**By Hash (Balanced)**
```
hash=0/, hash=1/, hash=2/
```
- Best for: Even distribution, avoiding skew
- Efficient for: Preventing hot partitions

**Tip**: Choose partition keys matching your most common filter conditions.

## Benefits of Partitioning

| Aspect | Benefit |
|--------|---------|
| **Query Speed** | Skip entire directories instead of scanning files |
| **I/O Reduction** | Only touch relevant partitions |
| **Parallelism** | Multiple workers can process different partitions |
| **Maintenance** | Delete/replace old partitions easily |
| **Incremental Writes** | Append new partitions without rewriting |

## Trade-offs

### More Granular Partitions
✓ **Pros**: Smaller files, faster individual queries, easier deletes
✗ **Cons**: More directories, slower full scans, overhead

### Coarser Partitions
✓ **Pros**: Fewer files, better full scans
✗ **Cons**: Larger files, slower targeted queries


## Performance Considerations

### Write Performance
- Unpartitioned: Fastest ✓
- Few partitions: Minimal overhead
- Many partitions: Slower (more I/O, more directories)

### Query Performance
- Fine queries (single day): Fastest with day partitions
- Range queries (month): Balanced with month partitions
- Full scans: Slowest with fine partitions

### Disk Space
- Same data = similar compressed size
- More directories = minor overhead


## Best Practices

1. **Choose keys matching query patterns**
   - Most common filters → partition keys
   - Rarely filtered → leave in files

2. **Avoid too many partitions**
   - Recommendation: 100-10,000 files total
   - Too many → overhead, slow full scans

3. **Use consistent naming**
   - Follow Hive convention: `key=value/`
   - Enable partition pruning in query engines

4. **Document your scheme**
   - Comment on design decisions
   - Record date partition format

5. **Plan for growth**
   - Start conservative
   - Repartition if needed

6. **Balance reads and writes**
   - Fine partitions: fast reads, complex writes
   - Coarse partitions: simple writes, slower reads


## Common Pitfalls

❌ **Avoid**: Storing partition columns in files
```python
# Bad:
df.to_parquet('data.parquet')  # Contains year, month, day columns
```

✓ **Do**: Remove partition columns
```python
# Good:
df.drop(['year', 'month', 'day'], axis=1).to_parquet('data.parquet')
```
