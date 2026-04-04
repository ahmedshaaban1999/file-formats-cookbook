"""
Metadata Inspection: Using parquet-tools and pyarrow to inspect file
statistics (min/max values, row counts) without loading the data.

This recipe demonstrates how to extract useful information from Parquet
files without paying the I/O cost of reading all the data.
"""

import pandas as pd
import pyarrow.parquet as pq
import numpy as np
import os
import shutil
from datetime import datetime


OUTPUT_DIR = 'output'


def setup_output_dir():
    """Set up clean output directory.
    
    Returns:
        meta_dir (string): Path to the metadata_inspection output directory.
    """
    meta_dir = os.path.join(OUTPUT_DIR, 'metadata_inspection')
    if os.path.exists(meta_dir):
        shutil.rmtree(meta_dir)
    os.makedirs(meta_dir, exist_ok=True)
    return meta_dir


def create_sample_dataset(num_rows=100000):
    """Create a sample dataset with diverse data types.
    
    Args:
        num_rows(integer): Number of rows to generate. Defaults to 100000.
    
    Returns:
        df (pd.DataFrame): A DataFrame with transaction data and mixed types.
    """
    print("Creating sample dataset...")
    
    data = {
        'transaction_id': np.arange(1, num_rows + 1),
        'user_id': np.random.randint(1000, 10000, num_rows),
        'amount': np.random.uniform(1, 10000, num_rows),
        'timestamp': pd.date_range('2023-01-01', periods=num_rows, freq='h'),
        'category': np.random.choice(
            ['Electronics', 'Clothing', 'Food', 'Books', 'Home'],
            num_rows
        ),
        'is_refunded': np.random.choice([True, False], num_rows),
        'payment_method': np.random.choice(
            ['Credit Card', 'Debit Card', 'PayPal', 'Bank Transfer'],
            num_rows
        )
    }
    
    df = pd.DataFrame(data)
    print(f"✓ Created dataset: {len(df):,} rows × {len(df.columns)} columns")
    
    return df


def write_parquet_multi_rowgroup(df, output_path, rows_per_group=10000):
    """Write Parquet file with multiple row groups.
    
    Args:
        df (pd.DataFrame): The Pandas DataFrame to write.
        output_path (string): Path where the Parquet file will be written.
        rows_per_group (integer): Target rows per row group. Defaults to 10000.
    
    Returns:
        output_path (string): Path to the written Parquet file.
    """
    print(f"\nWriting Parquet file with row groups (~{rows_per_group} rows each)...")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        row_group_size=rows_per_group
    )
    
    print(f"✓ Written to {output_path}")
    print(f"  File size: {os.path.getsize(output_path) / (1024**2):.2f} MB")
    
    return output_path


def inspect_file_metadata(parquet_file):
    """Inspect file-level metadata including schema and row group info.
    
    Args:
        parquet_file (pq.ParquetFile): Path to the Parquet file to inspect.
    
    Returns:
        - parquet_file_obj (pq.ParquetFile): File handler object
        - metadata (pq.FileMetaData): Metadata object for the file
        - schema (pq.ParquetSchema): Schema object
    """
    print("\n" + "=" * 60)
    print("File-Level Metadata")
    print("=" * 60)
    
    # Read Parquet file metadata
    parquet_file_obj = pq.ParquetFile(parquet_file)
    
    # Basic file info
    print(f"\nFile: {os.path.basename(parquet_file)}")
    print(f"  Size: {os.path.getsize(parquet_file) / (1024**2):.2f} MB")
    
    # Metadata
    metadata = parquet_file_obj.metadata
    print(f"\nTable Metadata:")
    print(f"  Total rows: {metadata.num_rows:,}")
    print(f"  Row groups: {metadata.num_row_groups}")
    print(f"  Compressed size: {metadata.serialized_size} bytes")
    
    # Schema
    schema = parquet_file_obj.schema
    print(f"\nSchema:")
    print(f"  Columns: {len(schema)}")
    for i, col in enumerate(schema):
        print(f"    {i+1}. {col.name}: {col.physical_type}")
    
    return parquet_file_obj, metadata, schema


def inspect_column_statistics(parquet_file_obj):
    """Inspect column statistics from metadata without reading data.
    
    Displays statistics for each column in each row group including type,
    encodings, and min/max values if available.
    
    Args:
        parquet_file_obj (pq.ParquetFile): File object to inspect.
    """
    print("\n" + "=" * 60)
    print("Column Statistics (from metadata, no data read)")
    print("=" * 60)
    
    metadata = parquet_file_obj.metadata
    
    for i in range(metadata.num_rows):
        pass  # Row group iteration
    
    # Get statistics for each row group
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        
        print(f"\nRow Group {rg_idx}:")
        print(f"  Rows: {row_group.num_rows:,}")
        print(f"  Bytes: {row_group.total_byte_size:,}")
        
        # Column statistics
        for col_idx in range(row_group.num_columns):
            col = row_group.column(col_idx)
            col_name = parquet_file_obj.schema.column(col_idx).name
            
            print(f"\n  {col_name}:")
            print(f"    Type: {col.physical_type}")
            print(f"    Encodings: {col.encodings}")
            print(f"    Compressed size: {col.total_compressed_size:,} bytes")
            print(f"    Uncompressed size: {col.total_uncompressed_size:,} bytes")
            
            # Statistics
            if col.is_stats_set:
                stats = col.statistics
                print(f"    Statistics available: ✓")
                if hasattr(stats, 'min') and hasattr(stats, 'max'):
                    try:
                        print(f"      Min: {stats.min}")
                        print(f"      Max: {stats.max}")
                    except:
                        print(f"      Min/Max: [binary data]")
            else:
                print(f"    Statistics available: ✗")


def get_column_statistics_summary(parquet_file_obj):
    """Extract useful column statistics across all row groups.
    
    Args:
        parquet_file_obj (pq.ParquetFile): File object to inspect.
    
    Returns:
        dict: Dictionary with column names as keys and statistics (type, min, max,
              range) as values for numeric columns.
    """
    print("\n" + "=" * 60)
    print("Column Statistics Summary")
    print("=" * 60)
    
    metadata = parquet_file_obj.metadata
    schema = parquet_file_obj.schema
    
    stats_summary = {}
    
    for col_idx in range(len(schema)):
        col_name = schema.column(col_idx).name
        col_type = schema.column(col_idx).physical_type
        
        # Collect stats from all row groups
        min_vals = []
        max_vals = []
        
        for rg_idx in range(metadata.num_row_groups):
            row_group = metadata.row_group(rg_idx)
            col = row_group.column(col_idx)
            
            if col.is_stats_set:
                stats = col.statistics
                if hasattr(stats, 'min') and hasattr(stats, 'max'):
                    try:
                        if isinstance(stats.min, (int, float)):
                            min_vals.append(stats.min)
                            max_vals.append(stats.max)
                    except:
                        pass
        
        if min_vals:
            stats_summary[col_name] = {
                'type': str(col_type),
                'min': min(min_vals),
                'max': max(max_vals),
                'range': max(max_vals) - min(min_vals)
            }
    
    print("\nNumeric Column Ranges:")
    for col_name, stats in stats_summary.items():
        print(f"  {col_name}:")
        print(f"    Type: {stats['type']}")
        print(f"    Min: {stats['min']}")
        print(f"    Max: {stats['max']}")
        print(f"    Range: {stats['range']}")
    
    return stats_summary


def demonstrate_predicate_filtering(parquet_file_obj):
    """Show how column statistics enable efficient predicate pushdown filtering.
    
    Demonstrates how min/max statistics allow the query engine to skip row
    groups that cannot contain matching data, reducing I/O cost.
    
    Args:
        parquet_file_obj (pq.ParquetFile): File object to inspect.
    """
    print("\n" + "=" * 60)
    print("Predicate Pushdown Benefits")
    print("=" * 60)
    
    metadata = parquet_file_obj.metadata
    schema = parquet_file_obj.schema
    
    print("\nScenario: Find transactions >= 5000")
    print("Without statistics: Read entire file")
    print("With statistics: Use min/max to skip row groups\n")
    
    query_min = 5000
    
    rows_skippable = 0
    rows_scannable = 0
    
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        
        # Find 'amount' column
        amount_col_idx = parquet_file_obj.schema_arrow.get_field_index('amount')
        col = row_group.column(amount_col_idx)
        
        if col.is_stats_set:
            stats = col.statistics
            col_max = stats.max
            
            if col_max < query_min:
                rows_skippable += row_group.num_rows
                status = "SKIP (max < 5000)"
            else:
                rows_scannable += row_group.num_rows
                status = "SCAN"
            
            print(f"  Row Group {rg_idx}: max={col_max:.0f} → {status}")
    
    total_rows = metadata.num_rows
    print(f"\nResult:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Can skip: {rows_skippable:,} ({100*rows_skippable/total_rows:.1f}%)")
    print(f"  Must scan: {rows_scannable:,} ({100*rows_scannable/total_rows:.1f}%)")


def demonstrate_schema_inspection(parquet_file_obj):
    """Show schema inspection and column details without reading data.
    
    Displays complete schema information including column types, nullability,
    and null counts extracted from metadata.
    
    Args:
        parquet_file_obj (pq.ParquetFile): File object to inspect.
    
    Returns:
        schema (pq.ParquetSchema): Schema object of the Parquet file.
    """
    print("\n" + "=" * 60)
    print("Schema Inspection (Zero Data Read)")
    print("=" * 60)
    
    schema = parquet_file_obj.schema
    metadata = parquet_file_obj.metadata
    
    print(f"\nDataset Overview:")
    print(f"  Total Rows: {metadata.num_rows:,}")
    print(f"  Columns: {len(schema)}")
    print(f"  Row Groups: {metadata.num_row_groups}")
    
    print(f"\nColumn Details:")
    for i in range(len(schema)):
        col = schema.column(i)
        col_name = col.name
        col_type = col.physical_type
        
        # Get nullability
        null_count = 0
        for rg_idx in range(metadata.num_row_groups):
            row_group = metadata.row_group(rg_idx)
            row_col = row_group.column(i)
            if row_col.is_stats_set:
                stats = row_col.statistics
                if hasattr(stats, 'null_count'):
                    null_count += stats.null_count
        
        print(f"  {i+1}. {col_name}")
        print(f"     Type: {col_type}")
        print(f"     Nulls: {null_count}")
    
    return schema


def estimate_memory_usage(parquet_file_obj, columns=None):
    """Estimate memory usage without loading all data.
    
    Uses uncompressed size metadata to estimate how much memory would be needed
    to load specific columns without actually reading the data.
    
    Args:
        parquet_file_obj (pq.ParquetFile): File object to inspect.
        columns: List of column names to estimate. If None, estimates all columns.
    
    Returns:
        total_uncompressed (integer): Total uncompressed size in bytes for the requested columns.
    """
    print("\n" + "=" * 60)
    print("Memory Estimation")
    print("=" * 60)
    
    metadata = parquet_file_obj.metadata
    schema = parquet_file_obj.schema
    
    if columns is None:
        columns = [schema.column(i).name for i in range(len(schema))]
    
    print(f"\nEstimated memory to read {len(columns)} column(s):")
    
    total_uncompressed = 0
    
    for col_name in columns:
        col_idx = parquet_file_obj.schema_arrow.get_field_index(col_name)
        uncompressed_size = 0
        
        for rg_idx in range(metadata.num_row_groups):
            row_group = metadata.row_group(rg_idx)
            col = row_group.column(col_idx)
            uncompressed_size += col.total_uncompressed_size
        
        uncompressed_mb = uncompressed_size / (1024**2)
        total_uncompressed += uncompressed_size
        
        print(f"  {col_name}: {uncompressed_mb:.2f} MB")
    
    print(f"\n  Total: {total_uncompressed / (1024**2):.2f} MB")
    print(f"  (Actual may vary with overhead)")
    
    return total_uncompressed


def main():
    print("=" * 60)
    print("Parquet Recipe 5: Metadata Inspection")
    print("=" * 60)
    
    meta_dir = setup_output_dir()
    
    # Create and write data
    df = create_sample_dataset(num_rows=100000)
    df = df.sort_values(by='amount')
    parquet_file = os.path.join(meta_dir, 'transactions.parquet')
    write_parquet_multi_rowgroup(df, parquet_file)
    
    # Open Parquet file
    parquet_file_obj = pq.ParquetFile(parquet_file)
    
    # Demonstrate metadata inspection
    inspect_file_metadata(parquet_file)
    inspect_column_statistics(parquet_file_obj)
    get_column_statistics_summary(parquet_file_obj)
    
    # Show practical benefits
    demonstrate_predicate_filtering(parquet_file_obj)
    demonstrate_schema_inspection(parquet_file_obj)
    
    # Memory estimation
    print("\nScenario 1: Read all columns")
    estimate_memory_usage(parquet_file_obj)
    
    print("\nScenario 2: Read only [transaction_id, amount, timestamp]")
    estimate_memory_usage(
        parquet_file_obj,
        columns=['transaction_id', 'amount', 'timestamp']
    )
    
    print("\nScenario 3: Read only [amount]")
    estimate_memory_usage(parquet_file_obj, columns=['amount'])
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print("✓ Metadata read without loading any rows")
    print("✓ Row counts and column statistics obtained instantly")
    print("✓ Predicate pushdown feasibility assessed")
    print("✓ Memory requirements estimated")
    print("=" * 60)


if __name__ == '__main__':
    main()
