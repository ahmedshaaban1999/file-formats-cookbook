# Recipe 5: Schema Evolution - Adding Columns Without Breaking Readers

## Overview

One of Parquet's most powerful features is its ability to evolve schemas gracefully. This recipe demonstrates how to add new columns to an existing dataset without breaking old readers. Learn how to maintain backward compatibility while incorporating new data fields into your pipelines.

## What You'll Learn

- How Parquet handles schema evolution
- Adding optional columns without breaking old code
- Combining files with different schemas
- Best practices for forward and backward compatibility
- Real-world migration strategies


## Key Concepts

### 1. **Backward Compatibility**
Old code reading new files always works:
```python
# Old file schema: [id, name, email, created_date, is_active]
# New file schema: [id, name, email, created_date, is_active, last_login, subscription]

# Old code:
v1_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_())
    ])

df_v1_pandas = pd.read_parquet(v2_file, schema=v1_schema, engine='pyarrow')
# Works perfectly! Simply ignores the new columns.
```

### 2. **Forward Compatibility**
New code reading old files with provided defaults:
```python
# Old file: [id, name, email, created_date, is_active]
# New schema: [id, name, email, created_date, is_active, last_login, subscription]

# New code:
v2_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_()),
        pa.field('last_login', pa.date64()),           # Added in V2
        pa.field('subscription_level', pa.string())     # Added in V2
    ])

table_v2_arrow = pq.read_table(v1_file, schema=v2_schema)
# last_login column: NULL (old file didn't have it)
# subscription column: NULL (needs default logic)
```

## Schema Evolution Strategy

### Phase 1: Add Without Breaking
```
Existing file: [id, name, email, created_at]
↓
New file: [id, name, email, created_at, last_login]

Old readers: Skip last_login, continue working
New readers: Read last_login or use default
```

### Phase 2: Gradual Migration
```
Day 1: Some partitions have new column
Day 2: Most partitions have new column
Day 7: All partitions upgraded

Old readers: Work throughout
New readers: See NULLs becoming rarer
```

### Phase 3: Clean Up (Optional)
```
After all consumers upgraded:
- Remove temporary NULL handling
- New readers can assume column exists
- Old files archived or rewritten
```

## Schema Merging Strategy

### Step 1: Read all files with the latest schema
Any missing columns will be populated by nulls
```
[id, name, email, created_at] -> found in both versions
[last_login, subscription_level] -> only in version 2, null values in version 1
```

### Step 2: Handle missing values
When filling NULL values, ensure column types are consistent
```python
if 'subscription_level' in merged_df.columns:
        merged_df['subscription_level'] = merged_df['subscription_level'].fillna('free')

if 'last_login' in merged_df.columns:
        merged_df['last_login'] = pd.to_datetime(merged_df['last_login'])
        merged_df['last_login'] = merged_df['last_login'].fillna(pd.to_datetime('2000-01-01'))
```


## Best Practices

1. **Plan schema from start**: Anticipate future columns
2. **Document changes**: Track migration phases
3. **Gradual rollout**: Stage new columns across time or partitions
4. **Set defaults**: Provide sensible defaults for new columns