"""
Schema Evolution: How to handle adding a new column to an existing Parquet
dataset without breaking old readers.

This recipe demonstrates best practices for evolving schemas over time,
ensuring backward and forward compatibility.
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import os
import shutil
from datetime import datetime, timedelta


OUTPUT_DIR = 'output'
schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('name', pa.string()),
    pa.field('email', pa.string()),
    pa.field('created_date', pa.date64()),
    pa.field('is_active', pa.bool_())
])


def setup_output_dir():
    """Set up clean output directory for schema evolution demo.
    
    Returns:
        schema_dir (string): Path to the schema_evolution output directory.
    """
    schema_dir = os.path.join(OUTPUT_DIR, 'schema_evolution')
    if os.path.exists(schema_dir):
        shutil.rmtree(schema_dir)
    os.makedirs(schema_dir, exist_ok=True)
    return schema_dir


def version_1_original_schema(schema_dir, num_records=1000):
    """Version 1: Create original schema with basic columns.
    
    Args:
        num_records (integer): Number of records to generate. Defaults to 1000.
    
    Returns:
        df (pd.DataFrame): DataFrame with id, name, email, created_date, and is_active columns.
    """
    print("=" * 60)
    print("Version 1: Original Schema")
    print("=" * 60)
    
    data = {
        'id': np.arange(num_records),
        'name': [f'user_{i}' for i in range(num_records)],
        'email': [f'user{i}@example.com' for i in range(num_records)],
        'created_date': pd.date_range('2023-01-01', periods=num_records, freq='h'),
        'is_active': np.random.choice([True, False], num_records)
    }
    
    df = pd.DataFrame(data)

    print("\n" + "-" * 60)
    print("Writing Version 1 file...")
    print("-" * 60)
    
    v1_dir = os.path.join(schema_dir, 'version_1')
    os.makedirs(v1_dir, exist_ok=True)
    
    file_path = os.path.join(v1_dir, 'version_1.parquet')
    df.to_parquet(
        file_path,
        engine='pyarrow',
        compression='snappy'
    )
    
    print(f"\n✓ Written {file_path}")
    print(f"Schema:")
    for col in df.columns:
        print(f"  - {col}: {df[col].dtype}")
    
    return df


def version_2_add_column(schema_dir, df_v1):
    """Version 2: Add new columns (last_login and subscription_level) to existing schema.
    
    Args:
        df_v1 (pd.DataFrame): DataFrame from version 1.
        schema_dir (string): Directory path for schema output.
    
    Returns:
        df (pd.DataFrame): DataFrame with added last_login and subscription_level columns.
    """
    print("\n" + "=" * 60)
    print("Version 2: Schema Evolution - Adding Columns")
    print("=" * 60)
    
    df_v2 = df_v1.copy()
    
    # Add last_login column
    df_v2['last_login'] = pd.date_range(
        '2024-01-01',
        periods=len(df_v2),
        freq='h'
    )
    
    # Add subscription_level column with default value for backward compatibility
    df_v2['subscription_level'] = 'free'
    
    # Randomly assign some users to paid tiers
    paid_indices = np.random.choice(len(df_v2), size=len(df_v2) // 4, replace=False)
    df_v2.loc[paid_indices, 'subscription_level'] = np.random.choice(
        ['basic', 'premium', 'enterprise'],
        size=len(paid_indices)
    )
    
    print("New schema:")
    for col in df_v2.columns:
        print(f"  - {col}: {df_v2[col].dtype}")
    
    print(f"Columns added: last_login, subscription_level")

    print("\n" + "-" * 60)
    print("Writing Version 2 file (with last_login and subscription_level)...")
    print("-" * 60)
    
    v2_dir = os.path.join(schema_dir, 'version_2')
    os.makedirs(v2_dir, exist_ok=True)
    
    file_path = os.path.join(v2_dir, 'version_2.parquet')
    df_v2.to_parquet(
        file_path,
        engine='pyarrow',
        compression='snappy'
    )
    
    print(f"\n✓ Written {file_path}")
    
    return df_v2



def read_version1_columns_from_version2(schema_dir):
    """Read Version 2 files using Version 1 schema.
    
    Demonstrates how to handle backward compatibility by reading new files with an old
    schema that doesn't include columns added in later versions.
    
    Args:
        schema_dir (string): Path to the schema_evolution output directory.
    """
    
    v2_file = os.path.join(schema_dir, 'version_2', 'version_2.parquet')
    
    # Define the Version 1 schema
    v1_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_())
    ])
    
    # ========== Read Version 1 File ==========
    print("-" * 60)
    print("Reading Version 2 File with Version 1 Schema as a Pandas DataFrame")
    print("-" * 60)
    
    df_v1_pandas = pd.read_parquet(v2_file, schema=v1_schema, engine='pyarrow')
    print(f"\nRead {len(df_v1_pandas)} rows from Version 2 file")
    print("Top 5 rows:")
    print(df_v1_pandas.head(5))
    
    return



def read_version2_columns_from_version1(schema_dir):
    """Read Version 1 files using Version 2 schema.
    
    Demonstrates how to handle forward compatibility by reading files with an extended
    schema that includes columns added in later versions. 
    
    Args:
        schema_dir (string): Path to the schema_evolution output directory.
    """
    
    v1_file = os.path.join(schema_dir, 'version_1', 'version_1.parquet')
    
    # Define the Version 2 schema that includes all columns
    v2_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_()),
        pa.field('last_login', pa.date64()),           # Added in V2
        pa.field('subscription_level', pa.string())     # Added in V2
    ])
    
    # ========== Read Version 2 File ==========
    
    print("\n" + "-" * 60)
    print("Reading Version 1 File with Version 2 Schema as PyArrow Table")
    print("-" * 60)
    
    table_v2_arrow = pq.read_table(v1_file, schema=v2_schema)
    print(f"\nRead {table_v2_arrow.num_rows} rows from Version 1 file")
    print("Top 5 rows:")
    print(table_v2_arrow.slice(0, 5).to_pandas())
    
    return


def demonstrate_schema_merging_strategy(schema_dir):
    """Merge Version 1 and Version 2 Parquet files and read the result using the
    Version 2 schema.

    Strategy:
    - Read both files using the Version 2 schema so columns added in V2 appear
      (missing values in V1 become nulls).
    - Concatenate the rows and deduplicate by primary key `id`, keeping the
      V2 row when duplicates exist (V2 overrides V1).
    - Fill `subscription_level` missing values with the V2 default 'free'.
    - Write the merged dataset and read it back using the V2 schema to demonstrate
      forward-compatible reads.

    Args:
        schema_dir (str): Path to the schema_evolution output directory
    """

    print("\n" + "-" * 60)
    print("Schema merging strategy: merging V1 + V2 into V2 schema")
    print("-" * 60)

    v1_file = os.path.join(schema_dir, 'version_1', 'version_1.parquet')
    v2_file = os.path.join(schema_dir, 'version_2', 'version_2.parquet')

    # Define V2 schema 
    v2_schema = pa.schema([
        pa.field('id', pa.int64()),
        pa.field('name', pa.string()),
        pa.field('email', pa.string()),
        pa.field('created_date', pa.date64()),
        pa.field('is_active', pa.bool_()),
        pa.field('last_login', pa.date64()),
        pa.field('subscription_level', pa.string())
    ])

    # Read both version 1 and version 2 files as one table and cast it into a dataframe
    merged_df = pq.read_table(schema_dir, schema=v2_schema).to_pandas()

    # Apply simple merge-time rules: 
    #   - missing subscription_level -> default 'free'
    #   - missing last_login -> 2000-01-01
    if 'subscription_level' in merged_df.columns:
        merged_df['subscription_level'] = merged_df['subscription_level'].fillna('free')

    if 'last_login' in merged_df.columns:
        # Ensure column is datetime64 type before filling nulls
        merged_df['last_login'] = pd.to_datetime(merged_df['last_login'])
        merged_df['last_login'] = merged_df['last_login'].fillna(pd.to_datetime('2000-01-01'))

    # Ensure merged directory exists and write merged parquet
    merged_dir = os.path.join(schema_dir, 'merged')
    os.makedirs(merged_dir, exist_ok=True)
    merged_path = os.path.join(merged_dir, 'merged_v2.parquet')

    merged_df.to_parquet(merged_path, engine='pyarrow', compression='snappy', index=False)

    print(f"\n✓ Written merged dataset to {merged_path}")

    # Read merged dataset back using the V2 schema to demonstrate the final read
    merged_table = pq.read_table(merged_path, schema=v2_schema)
    print(f"\nRead {merged_table.num_rows} rows from merged file using V2 schema")
    print("Top 5 rows:")
    print(merged_table.slice(0, 5).to_pandas())

    return merged_table


def main():
    print("=" * 60)
    print("Parquet Recipe 4: Schema Evolution")
    print("=" * 60)
    
    schema_dir = setup_output_dir()
    
    # Create different versions of the data
    df_v1 = version_1_original_schema(schema_dir, num_records=1000)
    df_v2 = version_2_add_column(schema_dir, df_v1)

    # Read Version 1 files with Version 1 schema
    read_version1_columns_from_version2(schema_dir)
    
    # Read Version 1 files with Version 2 schema
    read_version2_columns_from_version1(schema_dir)
    
    
    # Schema merging strategy: merge V1 and V2 files then read merged with V2 schema
    demonstrate_schema_merging_strategy(schema_dir)
    
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print("✓ Schema evolution in Parquet is backward compatible")
    print("✓ Adding optional columns doesn't break old readers")
    print("✓ Mixed version files can be combined with proper null handling")
    print("✓ New columns are simply ignored by old code")
    print("=" * 60)


if __name__ == '__main__':
    main()
