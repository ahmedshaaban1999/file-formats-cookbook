"""
Z-Ordering in Parquet: Multi-dimensional data clustering for efficient 
multi-column filtering and predicate pushdown optimization.

This recipe demonstrates how z-ordering arranges data in space-filling 
curve order to improve row group pruning for multi-column range queries.
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import os
import shutil
import time
from datetime import datetime


OUTPUT_DIR = 'output'


def setup_output_dir():
    """Set up clean output directory.
    
    Returns:
        z_dir (string): Path to the z_ordering output directory.
    """
    z_dir = os.path.join(OUTPUT_DIR, 'z_ordering')
    if os.path.exists(z_dir):
        shutil.rmtree(z_dir)
    os.makedirs(z_dir, exist_ok=True)
    return z_dir


def create_sample_dataset(num_rows=100000):
    """Create sample transaction data suitable for z-ordering.
    
    Creates a dataset with correlated dimensions (dates and amounts)
    that benefits from z-order clustering.
    
    Args:
        num_rows (integer): Number of rows to generate. Defaults to 100000.
    
    Returns:
        df (pd.DataFrame): Transaction data with date, amount, and other fields.
    """
    print("Creating sample dataset...")
    
    # Create dates (one per hour over ~3 months)
    dates = pd.date_range('2023-01-01', periods=num_rows, freq='h')
    
    # Create amounts with slight temporal correlation
    # Some periods have higher transaction amounts
    amount_base = 500 + 200 * np.sin(np.arange(num_rows) / 5000)
    amounts = amount_base + np.random.normal(0, 100, num_rows)
    amounts = np.clip(amounts, 10, 10000)
    
    data = {
        'transaction_id': np.arange(1, num_rows + 1),
        'transaction_date': dates,
        'amount': amounts,
        'customer_id': np.random.randint(1000, 5000, num_rows),
        'category': np.random.choice(
            ['Electronics', 'Groceries', 'Clothing', 'Travel', 'Entertainment'],
            num_rows
        ),
        'status': np.random.choice(['completed', 'pending', 'cancelled'], num_rows)
    }
    
    df = pd.DataFrame(data)
    print(f"✓ Created dataset: {len(df):,} rows × {len(df.columns)} columns")
    
    return df


def compute_z_order_index(df, columns, bits=8):
    """Compute z-order (Morton) indices for multi-dimensional data.
    
    Z-ordering interleaves the bits of normalized column values to create
    a space-filling curve that preserves multi-dimensional locality.
    
    Args:
        df (pd.DataFrame): DataFrame to compute z-order for.
        columns: List of column names to use for z-ordering.
        bits (integer): Number of bits for normalization (default 8 = 0-255).
    
    Returns:
        z_indices (np.ndarray): Z-order index values for each row.
    """
    print(f"\nComputing z-order indices for columns: {columns}")
    
    z_indices = np.zeros(len(df), dtype=np.int64)
    max_val = (1 << bits) - 1  # 2^bits - 1
    
    for col_idx, col in enumerate(columns):
        col_data = df[col].values
        
        # Normalize column to range [0, 2^bits - 1]
        if col_data.dtype == 'datetime64[ns]':
            # Convert datetime to numeric for normalization
            col_numeric = col_data.astype(np.int64)
        else:
            col_numeric = col_data.astype(np.float64)
        
        col_min = col_numeric.min()
        col_max = col_numeric.max()
        
        if col_max > col_min:
            normalized = ((col_numeric - col_min) / (col_max - col_min) * max_val).astype(np.int64)
        else:
            normalized = np.zeros(len(col_data), dtype=np.int64)
        
        # Interleave bits from this column
        for bit_pos in range(bits):
            bit_mask = ((normalized >> bit_pos) & 1).astype(np.int64)
            z_indices |= bit_mask << (col_idx * bits + bit_pos)
    
    print(f"✓ Z-order indices computed")
    return z_indices


def write_parquet_with_z_ordering(df, output_path, z_columns, row_group_size=10000):
    """Write Parquet file with z-order clustering.
    
    Sorts data by z-order index before writing to enable efficient
    multi-column filtering and row group pruning.
    
    Args:
        df (pd.DataFrame): DataFrame to write.
        output_path (string): Path where Parquet file will be written.
        z_columns: List of columns to use for z-ordering.
        row_group_size (integer): Rows per row group. Defaults to 10000.
    
    Returns:
        output_path (string): Path to the written file.
    """
    print(f"\nWriting z-ordered Parquet file...")
    print(f"  Z-order columns: {z_columns}")
    print(f"  Row group size: {row_group_size:,} rows")
    
    # Compute z-order indices
    z_indices = compute_z_order_index(df, z_columns)
    
    # Sort by z-order
    sorted_indices = np.argsort(z_indices)
    df_sorted = df.iloc[sorted_indices].reset_index(drop=True)
    
    # Write to Parquet
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    table = pa.Table.from_pandas(df_sorted)
    pq.write_table(
        table,
        output_path,
        row_group_size=row_group_size,
        compression='snappy'
    )
    
    file_size_mb = os.path.getsize(output_path) / (1024**2)
    print(f"✓ Written to {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    return output_path


def write_parquet_without_z_ordering(df, output_path, row_group_size=10000):
    """Write Parquet file without z-order clustering (baseline).
    
    Data is kept in original order for comparison with z-ordered version.
    
    Args:
        df (pd.DataFrame): DataFrame to write.
        output_path (string): Path where Parquet file will be written.
        row_group_size (integer): Rows per row group. Defaults to 10000.
    
    Returns:
        output_path (string): Path to the written file.
    """
    print(f"\nWriting baseline Parquet file (no z-ordering)...")
    print(f"  Row group size: {row_group_size:,} rows")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    table = pa.Table.from_pandas(df)
    pq.write_table(
        table,
        output_path,
        row_group_size=row_group_size,
        compression='snappy'
    )
    
    file_size_mb = os.path.getsize(output_path) / (1024**2)
    print(f"✓ Written to {output_path}")
    print(f"  File size: {file_size_mb:.2f} MB")
    
    return output_path


def analyze_row_group_statistics(parquet_file, z_ordered=False):
    """Analyze row group statistics for filtering efficiency.
    
    Examines min/max values in key columns across row groups to assess
    how effectively z-ordering improves row group pruning.
    
    Args:
        parquet_file (string): Path to Parquet file to analyze.
        z_ordered (boolean): Whether this is a z-ordered file (for labeling).
    
    Returns:
        list: List of row group statistics.
    """
    label = "Z-Ordered" if z_ordered else "Baseline"
    print(f"\n{'='*60}")
    print(f"{label} - Row Group Statistics")
    print(f"{'='*60}")
    
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.metadata
    schema_arrow = pf.schema_arrow
    
    stats_list = []
    
    # Get column indices
    date_col_idx = schema_arrow.get_field_index('transaction_date')
    amount_col_idx = schema_arrow.get_field_index('amount')
    
    print(f"\nRow Group Details (Date and Amount ranges):\n")
    
    for rg_idx in range(metadata.num_row_groups):
        row_group = metadata.row_group(rg_idx)
        
        # Get date statistics
        date_col = row_group.column(date_col_idx)
        amount_col = row_group.column(amount_col_idx)
        
        date_stats = date_col.statistics if date_col.is_stats_set else None
        amount_stats = amount_col.statistics if amount_col.is_stats_set else None
        
        if date_stats and amount_stats:
            # Convert timestamp to readable format
            date_min = pd.Timestamp(date_stats.min, unit='ns')
            date_max = pd.Timestamp(date_stats.max, unit='ns')
            amount_min = amount_stats.min
            amount_max = amount_stats.max
            
            stats_list.append({
                'rg_idx': rg_idx,
                'rows': row_group.num_rows,
                'date_min': date_min,
                'date_max': date_max,
                'amount_min': amount_min,
                'amount_max': amount_max
            })
            
            print(f"RG {rg_idx}:")
            print(f"  Rows: {row_group.num_rows:,}")
            print(f"  Dates: {date_min.date()} → {date_max.date()}")
            print(f"  Amounts: ${amount_min:.0f} → ${amount_max:.0f}")
            print()
    
    return stats_list


def demonstrate_filtering_efficiency(baseline_stats, z_ordered_stats):
    """Demonstrate filtering efficiency improvements with z-ordering.
    
    Simulates predicate filtering (e.g., date >= X AND amount >= Y) and
    counts how many row groups can be skipped in each case.
    
    Args:
        baseline_stats: List of statistics from baseline (non z-ordered) file.
        z_ordered_stats: List of statistics from z-ordered file.
    """
    print(f"\n{'='*60}")
    print(f"Filtering Efficiency Comparison")
    print(f"{'='*60}")
    
    # Define filter: dates after Jan 15 AND amounts >= $600
    filter_date = pd.Timestamp('2023-01-15')
    filter_amount = 600
    
    print(f"\nFilter: transaction_date >= {filter_date.date()} AND amount >= ${filter_amount}")
    print()
    
    def count_scannable_rgs(stats_list):
        """Count row groups that must be scanned based on filter."""
        scannable = 0
        skippable = 0
        
        for stats in stats_list:
            # Can skip if all dates are before filter_date AND all amounts < filter_amount
            can_skip_date = stats['date_max'] < filter_date
            can_skip_amount = stats['amount_max'] < filter_amount
            
            if can_skip_date or can_skip_amount:
                skippable += 1
            else:
                scannable += 1
        
        return scannable, skippable
    
    baseline_scan, baseline_skip = count_scannable_rgs(baseline_stats)
    z_ordered_scan, z_ordered_skip = count_scannable_rgs(z_ordered_stats)
    
    total_rgs = len(baseline_stats)
    baseline_skip_pct = 100 * baseline_skip / total_rgs if total_rgs > 0 else 0
    z_ordered_skip_pct = 100 * z_ordered_skip / total_rgs if total_rgs > 0 else 0
    
    print(f"Baseline (no z-ordering):")
    print(f"  Must scan: {baseline_scan}/{total_rgs} row groups ({100*baseline_scan/total_rgs:.1f}%)")
    print(f"  Can skip:  {baseline_skip}/{total_rgs} row groups ({baseline_skip_pct:.1f}%)")
    
    print(f"\nZ-Ordered:")
    print(f"  Must scan: {z_ordered_scan}/{total_rgs} row groups ({100*z_ordered_scan/total_rgs:.1f}%)")
    print(f"  Can skip:  {z_ordered_skip}/{total_rgs} row groups ({z_ordered_skip_pct:.1f}%)")
    
    if z_ordered_scan < baseline_scan:
        improvement = 100 * (baseline_scan - z_ordered_scan) / z_ordered_scan
        print(f"\n✓ Z-ordering improves row group pruning by {improvement:.1f}%")
    else:
        print(f"\n  Row group pruning is similar in both cases")


def main():
    print("=" * 60)
    print("Parquet Recipe: Z-Ordering for Multi-Column Filtering")
    print("=" * 60)
    
    z_dir = setup_output_dir()
    
    # Create sample data
    df = create_sample_dataset(num_rows=100000)
    
    print(f"\nDataset Summary:")
    print(f"  Date range: {df['transaction_date'].min().date()} → {df['transaction_date'].max().date()}")
    print(f"  Amount range: ${df['amount'].min():.2f} → ${df['amount'].max():.2f}")
    print(f"  Mean amount: ${df['amount'].mean():.2f}")
    
    # Write baseline file (no z-ordering)
    baseline_file = os.path.join(z_dir, 'transactions_baseline.parquet')
    write_parquet_without_z_ordering(df, baseline_file)
    
    # Write z-ordered file
    z_ordered_file = os.path.join(z_dir, 'transactions_z_ordered.parquet')
    write_parquet_with_z_ordering(df, z_ordered_file, ['transaction_date', 'amount'])
    
    # Analyze both files
    baseline_stats = analyze_row_group_statistics(baseline_file, z_ordered=False)
    z_ordered_stats = analyze_row_group_statistics(z_ordered_file, z_ordered=True)
    
    # Compare filtering efficiency
    demonstrate_filtering_efficiency(baseline_stats, z_ordered_stats)
    
    # Summary
    print(f"\n{'='*60}")
    print("Summary")
    print(f"{'='*60}")
    print("✓ Z-ordering clusters multi-dimensional data")
    print("✓ Improves row group pruning for multi-column filters")
    print("✓ Most effective with 2-3 correlated dimensions")
    print("✓ Trade-off: pre-sorting cost vs. query performance gain")
    print("=" * 60)


if __name__ == '__main__':
    main()
