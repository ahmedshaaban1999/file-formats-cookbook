"""
Partitioning: Writing data partitioned by year/month/day to disk
to optimize directory scanning and query performance.

This recipe demonstrates how to organize large Parquet datasets into
partitioned directories for efficient querying by time range.
"""

import pandas as pd
import numpy as np
import os
import shutil
from datetime import datetime, timedelta
import time


OUTPUT_BASE_DIR = 'output'


def create_time_series_data(days=365, records_per_day=1000):
    """Create a time-series dataset spanning multiple days.
    
    Args:
        days (integer): Number of days to generate data for. Defaults to 365.
        records_per_day (integer): Number of records to create per day. Defaults to 1000.
    
    Returns:
        df (pd.DataFrame): Time-series data with timestamp, user_id, value, and category columns.
    """
    print(f"Creating time-series data for {days} days...")
    
    dates = []
    users = []
    values = []
    categories = []
    
    start_date = datetime(2023, 1, 1)
    
    for day in range(days):
        current_date = start_date + timedelta(days=day)
        
        for _ in range(records_per_day):
            dates.append(current_date)
            users.append(np.random.randint(1000, 5000))
            values.append(np.random.randn() * 100 + 500)
            categories.append(np.random.choice(['A', 'B', 'C', 'D']))
    
    df = pd.DataFrame({
        'timestamp': dates,
        'user_id': users,
        'value': values,
        'category': categories
    })
    
    print(f"✓ Created {len(df):,} records from {start_date.date()} to {(start_date + timedelta(days=days-1)).date()}")
    
    return df


def setup_output_dir():
    """Set up clean output directories."""
    if os.path.exists(OUTPUT_BASE_DIR):
        shutil.rmtree(OUTPUT_BASE_DIR)
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)


def write_unpartitioned(df, output_path):
    """Write all data to a single Parquet file (baseline for comparison).
    
    Args:
        df (pd.DataFrame): The Pandas DataFrame to write.
        output_path (string): Directory path where the Parquet file will be written.
    
    Returns:
        - elapsed_time (float): Write time in seconds
        - file_size_mb (float): File size in megabytes
    """
    print("\n" + "=" * 60)
    print("Scenario 1: Unpartitioned (All data in single file)")
    print("=" * 60)
    
    output_file = os.path.join(output_path, 'data.parquet')
    os.makedirs(output_path, exist_ok=True)
    
    start_time = time.time()
    df.to_parquet(output_file, engine='pyarrow', compression='snappy')
    elapsed = time.time() - start_time
    
    file_size = os.path.getsize(output_file) / (1024**2)
    print(f"✓ Written {len(df):,} records to single file")
    print(f"  Write time: {elapsed:.3f}s")
    print(f"  File size: {file_size:.2f} MB")
    print(f"  Location: {output_file}")
    
    return elapsed, file_size


def write_partitioned_by_year_month(df, output_path):
    """Write data partitioned by year and month directories.
    
    Args:
        df (pd.DataFrame): The Pandas DataFrame to write with timestamp column.
        output_path (string): Base directory path for partitioned output.
    
    Returns:
        - elapsed_time (float): Write time in seconds
        - total_size_mb (float): Total size of all partition files in MB
        - file_count (integer): Number of partition files created
    """
    print("\n" + "=" * 60)
    print("Scenario 2: Partitioned by Year/Month")
    print("=" * 60)
    
    partitioned_dir = os.path.join(output_path, 'by_year_month')
    os.makedirs(partitioned_dir, exist_ok=True)
    
    # Add year and month columns for partitioning
    df_temp = df.copy()
    df_temp['year'] = df_temp['timestamp'].dt.year
    df_temp['month'] = df_temp['timestamp'].dt.month
    
    start_time = time.time()
    
    # Write each partition as a separate file
    file_count = 0
    for (year, month), group_df in df_temp.groupby(['year', 'month']):
        partition_dir = os.path.join(
            partitioned_dir, 
            f'year={year}', 
            f'month={month:02d}'
        )
        os.makedirs(partition_dir, exist_ok=True)
        
        output_file = os.path.join(partition_dir, 'data.parquet')
        group_df.drop(['year', 'month'], axis=1).to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy'
        )
        file_count += 1
    
    elapsed = time.time() - start_time
    
    # Calculate total size
    total_size = sum(
        os.path.getsize(os.path.join(root, f)) 
        for root, _, files in os.walk(partitioned_dir)
        for f in files
    ) / (1024**2)
    
    print(f"✓ Written {len(df):,} records across {file_count} partition files")
    print(f"  Write time: {elapsed:.3f}s")
    print(f"  Total size: {total_size:.2f} MB")
    print(f"  Files per partition: {file_count // 12} to {(file_count + 11) // 12}")
    print(f"  Location: {partitioned_dir}")
    
    return elapsed, total_size, file_count


def write_partitioned_by_date(df, output_path):
    """Write data partitioned by year, month, and day directories.
    
    Args:
        df (pd.DataFrame): The Pandas DataFrame to write with timestamp column.
        output_path (string): Base directory path for partitioned output.
    
    Returns:
        - elapsed_time (float): Write time in seconds
        - total_size_mb (float): Total size of all partition files in MB
        - file_count (integer): Number of partition files created
    """
    print("\n" + "=" * 60)
    print("Scenario 3: Partitioned by Year/Month/Day")
    print("=" * 60)
    
    partitioned_dir = os.path.join(output_path, 'by_date')
    os.makedirs(partitioned_dir, exist_ok=True)
    
    # Add date components for partitioning
    df_temp = df.copy()
    df_temp['year'] = df_temp['timestamp'].dt.year
    df_temp['month'] = df_temp['timestamp'].dt.month
    df_temp['day'] = df_temp['timestamp'].dt.day
    
    start_time = time.time()
    
    # Write each partition as a separate file
    file_count = 0
    for (year, month, day), group_df in df_temp.groupby(['year', 'month', 'day']):
        partition_dir = os.path.join(
            partitioned_dir,
            f'year={year}',
            f'month={month:02d}',
            f'day={day:02d}'
        )
        os.makedirs(partition_dir, exist_ok=True)
        
        output_file = os.path.join(partition_dir, 'data.parquet')
        group_df.drop(['year', 'month', 'day'], axis=1).to_parquet(
            output_file,
            engine='pyarrow',
            compression='snappy'
        )
        file_count += 1
    
    elapsed = time.time() - start_time
    
    # Calculate total size
    total_size = sum(
        os.path.getsize(os.path.join(root, f))
        for root, _, files in os.walk(partitioned_dir)
        for f in files
    ) / (1024**2)
    
    print(f"✓ Written {len(df):,} records across {file_count} partition files")
    print(f"  Write time: {elapsed:.3f}s")
    print(f"  Total size: {total_size:.2f} MB")
    print(f"  Records per file: ~{len(df) // file_count:,}")
    print(f"  Location: {partitioned_dir}")
    
    return elapsed, total_size, file_count


# Single Date Read Functions

def read_single_date_unpartitioned(base_path, target_date='2023-03-15'):
    """Read data for a specific date from unpartitioned file (full scan).
    
    Args:
        base_path (string): Base directory path containing the data.
        target_date (string): Target date in YYYY-MM-DD format. Defaults to '2023-03-15'.
    
    Returns:
        - dataframe (pd.DataFrame): Data for the target date (filtered)
        - elapsed_time (float): Read time in seconds
    """
    parquet_file = os.path.join(base_path, 'unpartitioned', 'data.parquet')
    
    start_time = time.time()
    df_full = pd.read_parquet(parquet_file, engine='pyarrow')
    # Filter to specific date
    target_dt = pd.to_datetime(target_date)
    df = df_full[df_full['timestamp'].dt.date == target_dt.date()]
    elapsed = time.time() - start_time
    
    return df, elapsed


def read_single_date_year_month(base_path, target_date='2023-03-15'):
    """Read data for a specific date from year/month partitioned data.
    
    Args:
        base_path (string): Base directory path containing the data.
        target_date (string): Target date in YYYY-MM-DD format. Defaults to '2023-03-15'.
    
    Returns:
        - dataframe (pd.DataFrame): Data for the target date (filtered)
        - elapsed_time (float): Read time in seconds
    """
    target_dt = pd.to_datetime(target_date)
    year = target_dt.year
    month = target_dt.month
    
    partition_path = os.path.join(
        base_path, 'by_year_month',
        f'year={year}', f'month={month:02d}',
        'data.parquet'
    )
    
    start_time = time.time()
    df_month = pd.read_parquet(partition_path, engine='pyarrow')
    # Filter to specific date
    df = df_month[df_month['timestamp'].dt.date == target_dt.date()]
    elapsed = time.time() - start_time
    
    return df, elapsed


def read_single_date_by_date(base_path, target_date='2023-03-15'):
    """Read data for a specific date from date-partitioned data (direct access).
    
    Args:
        base_path (string): Base directory path containing the data.
        target_date (string): Target date in YYYY-MM-DD format. Defaults to '2023-03-15'.
    
    Returns:
        - dataframe (pd.DataFrame): Data for the target date
        - elapsed_time (float): Read time in seconds
    """
    target_dt = pd.to_datetime(target_date)
    year = target_dt.year
    month = target_dt.month
    day = target_dt.day
    
    partition_path = os.path.join(
        base_path, 'by_date',
        f'year={year}', f'month={month:02d}', f'day={day:02d}',
        'data.parquet'
    )
    
    start_time = time.time()
    df = pd.read_parquet(partition_path, engine='pyarrow')
    elapsed = time.time() - start_time
    
    return df, elapsed


def compare_single_date_reads(base_path, target_date='2023-03-15'):
    """Compare reading a single date across all three partitioning schemes.
    
    Args:
        base_path (string): Base directory path containing the partitioned data.
        target_date (string): Target date in YYYY-MM-DD format. Defaults to '2023-03-15'.
    
    Returns:
        Dictionary with read times for each scenario.
    """
    print("\n" + "=" * 60)
    print(f"Query: Reading data for single date ({target_date})")
    print("=" * 60)
    
    # Unpartitioned read
    print("\n[1] Reading from unpartitioned data (full table scan)...")
    df_unpart, time_unpart = read_single_date_unpartitioned(base_path, target_date)
    print(f"    ✓ Read {len(df_unpart):,} records in {time_unpart:.3f}s")
    
    # Year/Month partitioned read
    print("[2] Reading from year/month partitioned data...")
    df_ym, time_ym = read_single_date_year_month(base_path, target_date)
    print(f"    ✓ Read {len(df_ym):,} records in {time_ym:.3f}s")
    
    # Date partitioned read
    print("[3] Reading from date partitioned data (direct access)...")
    df_date, time_date = read_single_date_by_date(base_path, target_date)
    print(f"    ✓ Read {len(df_date):,} records in {time_date:.3f}s")
    
    # Summary
    print("\nPerformance Comparison:")
    print(f"  Unpartitioned (baseline):  {time_unpart:.3f}s")
    print(f"  Year/Month partitioned:    {time_ym:.3f}s ({(time_unpart/time_ym):.2f}x faster)")
    print(f"  Date partitioned:          {time_date:.3f}s ({(time_unpart/time_date):.2f}x faster)")
    
    return {'unpartitioned': time_unpart, 'year_month': time_ym, 'by_date': time_date}


# Month Range Read Functions

def read_month_range_unpartitioned(base_path, months=None):
    """Read data for a month range from unpartitioned file (full scan + filter).
    
    Args:
        base_path (string): Base directory path containing the data.
        months (list): Months to read (1-12). Defaults to [1, 2, 3] for Q1.
    
    Returns:
        - dataframe (pd.DataFrame): Data for the target months (filtered)
        - elapsed_time (float): Read time in seconds
    """
    if months is None:
        months = [1, 2, 3]
    
    parquet_file = os.path.join(base_path, 'unpartitioned', 'data.parquet')
    
    start_time = time.time()
    df_full = pd.read_parquet(parquet_file, engine='pyarrow')
    # Filter to specific months in 2023
    df = df_full[(df_full['timestamp'].dt.year == 2023) & 
                 (df_full['timestamp'].dt.month.isin(months))]
    elapsed = time.time() - start_time
    
    return df, elapsed


def read_month_range_year_month(base_path, months=None):
    """Read data for a month range from year/month partitioned data.
    
    Args:
        base_path (string): Base directory path containing the data.
        months (list): Months to read (1-12). Defaults to [1, 2, 3] for Q1.
    
    Returns:
        - dataframe (pd.DataFrame): Data for the target months
        - elapsed_time (float): Read time in seconds
    """
    if months is None:
        months = [1, 2, 3]
    
    dfs = []
    start_time = time.time()
    
    for month in months:
        partition_path = os.path.join(
            base_path, 'by_year_month',
            'year=2023', f'month={month:02d}',
            'data.parquet'
        )
        dfs.append(pd.read_parquet(partition_path, engine='pyarrow'))
    
    df = pd.concat(dfs, ignore_index=True)
    elapsed = time.time() - start_time
    
    return df, elapsed


def read_month_range_by_date(base_path, months=None):
    """Read data for a month range from date-partitioned data.
    
    Args:
        base_path (string): Base directory path containing the data.
        months (list): Months to read (1-12). Defaults to [1, 2, 3] for Q1.
    
    Returns:
        - dataframe (pd.DataFrame): Data for the target months
        - elapsed_time (float): Read time in seconds
    """
    if months is None:
        months = [1, 2, 3]
    
    dfs = []
    start_time = time.time()
    
    for month in months:
        partition_path = os.path.join(
            base_path, 'by_date',
            'year=2023', f'month={month:02d}'
        )
        
        # Read all days in this month
        for day_dir in os.listdir(partition_path):
            if day_dir.startswith('day='):
                parquet_file = os.path.join(
                    partition_path, day_dir, 'data.parquet'
                )
                dfs.append(pd.read_parquet(parquet_file, engine='pyarrow'))
    
    df = pd.concat(dfs, ignore_index=True)
    elapsed = time.time() - start_time
    
    return df, elapsed


def compare_month_range_reads(base_path, months=None):
    """Compare reading a month range across all three partitioning schemes.
    
    Args:
        base_path (string): Base directory path containing the partitioned data.
        months (list): Months to read (1-12). Defaults to [1, 2, 3] for Q1.
    
    Returns:
        Dictionary with read times for each scenario.
    """
    if months is None:
        months = [1, 2, 3]
    
    month_str = ', '.join([f"month {m:02d}" for m in months])
    print("\n" + "=" * 60)
    print(f"Query: Reading Q{(months[0]-1)//3 + 1} 2023 ({month_str})")
    print("=" * 60)
    
    # Unpartitioned read
    print("\n[1] Reading from unpartitioned data (full table scan + filter)...")
    df_unpart, time_unpart = read_month_range_unpartitioned(base_path, months)
    print(f"    ✓ Read {len(df_unpart):,} records in {time_unpart:.3f}s")
    
    # Year/Month partitioned read
    print("[2] Reading from year/month partitioned data...")
    df_ym, time_ym = read_month_range_year_month(base_path, months)
    print(f"    ✓ Read {len(df_ym):,} records in {time_ym:.3f}s")
    
    # Date partitioned read
    print("[3] Reading from date partitioned data...")
    df_date, time_date = read_month_range_by_date(base_path, months)
    print(f"    ✓ Read {len(df_date):,} records in {time_date:.3f}s")
    
    # Summary
    print("\nPerformance Comparison:")
    print(f"  Unpartitioned (baseline):  {time_unpart:.3f}s")
    print(f"  Year/Month partitioned:    {time_ym:.3f}s ({(time_unpart/time_ym):.2f}x faster)")
    print(f"  Date partitioned:          {time_date:.3f}s ({(time_unpart/time_date):.2f}x faster)")
    
    return {'unpartitioned': time_unpart, 'year_month': time_ym, 'by_date': time_date}



def main():
    print("=" * 60)
    print("Parquet Recipe 3: Partitioning for Query Optimization")
    print("=" * 60)
    
    setup_output_dir()
    output_path = os.path.join(OUTPUT_BASE_DIR, 'partitioning_demo')
    
    # Create data
    df = create_time_series_data(days=365, records_per_day=10000)
    
    # Scenario 1: Unpartitioned
    unpart_time, unpart_size = write_unpartitioned(df, os.path.join(output_path, 'unpartitioned'))
    
    # Scenario 2: Partitioned by Year/Month
    part_ym_time, part_ym_size, part_ym_files = write_partitioned_by_year_month(
        df,
        output_path
    )
    
    # Scenario 3: Partitioned by Year/Month/Day
    part_date_time, part_date_size, part_date_files = write_partitioned_by_date(
        df,
        output_path
    )
    
    # Query performance comparisons
    print("\n" + "=" * 60)
    print("Query Performance Comparisons")
    print("=" * 60)
    
    single_date_times = compare_single_date_reads(output_path)
    month_range_times = compare_month_range_reads(output_path)
    
    # Summary
    print("\n" + "=" * 60)
    print("Performance Summary")
    print("=" * 60)
    
    print("\nWrite Performance:")
    print(f"  Unpartitioned:           {unpart_time:.3f}s (baseline)")
    print(f"  Year/Month partitioned:  {part_ym_time:.3f}s ({(unpart_time/part_ym_time):.2f}x)")
    print(f"  Date partitioned:        {part_date_time:.3f}s ({(unpart_time/part_date_time):.2f}x)")
    
    print("\nStorage Size:")
    print(f"  Unpartitioned:           {unpart_size:.2f} MB (baseline)")
    print(f"  Year/Month partitioned:  {part_ym_size:.2f} MB ({((unpart_size-part_ym_size)/unpart_size)*100:.1f}% change)")
    print(f"  Date partitioned:        {part_date_size:.2f} MB ({((unpart_size-part_date_size)/unpart_size)*100:.1f}% change)")
    
    print("\n" + "=" * 60)
    print("Query Performance Analysis")
    print("=" * 60)
    
    print("\nSingle Date Query (2023-03-15):")
    print(f"  Unpartitioned:           {single_date_times['unpartitioned']:.3f}s (baseline)")
    print(f"  Year/Month partitioned:  {single_date_times['year_month']:.3f}s ({(single_date_times['unpartitioned']/single_date_times['year_month']):.2f}x faster)")
    print(f"  Date partitioned:        {single_date_times['by_date']:.3f}s ({(single_date_times['unpartitioned']/single_date_times['by_date']):.2f}x faster)")
    
    print("\nMonth Range Query (Q1 2023):")
    print(f"  Unpartitioned:           {month_range_times['unpartitioned']:.3f}s (baseline)")
    print(f"  Year/Month partitioned:  {month_range_times['year_month']:.3f}s ({(month_range_times['unpartitioned']/month_range_times['year_month']):.2f}x faster)")
    print(f"  Date partitioned:        {month_range_times['by_date']:.3f}s ({(month_range_times['unpartitioned']/month_range_times['by_date']):.2f}x faster)")



if __name__ == '__main__':
    main()
