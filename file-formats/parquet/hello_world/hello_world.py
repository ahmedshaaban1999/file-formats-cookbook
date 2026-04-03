"""
Hello World: Writing a Pandas DataFrame to Parquet using fastparquet vs. pyarrow engines

This recipe demonstrates the basic workflow of converting a Pandas DataFrame
to Parquet format using both available engines and comparing the output.
"""

import pandas as pd
import numpy as np
import time
import os

def create_sample_data(num_rows=100000):
    """Create a sample, populated DataFrame with synthetic employee data.
    
    Args:
        num_rows (integer): Number of rows to generate. Defaults to 100000.
    
    Returns:
        pd.DataFrame: A DataFrame containing synthesized employee records.
    """
    data = {
        'id': np.arange(num_rows),
        'name': [f'user_{i}' for i in range(num_rows)],
        'age': np.random.randint(18, 100, num_rows),
        'salary': np.random.randint(30000, 150000, num_rows),
        'department': np.random.choice(['Sales', 'Engineering', 'HR', 'Marketing'], num_rows),
        'hire_date': pd.date_range('2015-01-01', periods=num_rows, freq='h')
    }
    return pd.DataFrame(data)


def write_with_pyarrow(df, filepath):
    """Write a DataFrame to Parquet using the PyArrow engine.
    
    Args:
        df (pd.DataFrame): The Pandas DataFrame to write.
        filepath (string): Output path for the Parquet file.
    
    Returns:
        - elapsed (float): Time in seconds to write the file.
        - file_size (float): Size of the written file in MB.
    """
    print("\nWriting with PyArrow engine...")
    start_time = time.time()
    df.to_parquet(filepath, engine='pyarrow', compression='snappy')
    elapsed = time.time() - start_time
    file_size = os.path.getsize(filepath) / (1024 * 1024)  # Convert to MB
    print(f"✓ Completed in {elapsed:.3f} seconds")
    print(f"  File size: {file_size:.2f} MB")
    return elapsed, file_size



def write_with_fastparquet(df, filepath):
    """Write a DataFrame to Parquet using the FastParquet engine.
    
    Args:
        df (pd.DataFrame): The Pandas DataFrame to write.
        filepath (string): Output path for the Parquet file.
    
    Returns:
        - elapsed (float): Time in seconds to write the file.
        - file_size (float): Size of the written file in MB.
    """
    print("\nWriting with FastParquet engine...")
    start_time = time.time()
    df.to_parquet(filepath, engine='fastparquet', compression='snappy')
    elapsed = time.time() - start_time
    file_size = os.path.getsize(filepath) / (1024 * 1024)  # Convert to MB
    print(f"✓ Completed in {elapsed:.3f} seconds")
    print(f"  File size: {file_size:.2f} MB")
    return elapsed, file_size


def read_parquet(filepath, engine='pyarrow'):
    """Read a Parquet file back into memory.
    
    Args:
        filepath (string): Path to the Parquet file to read.
        engine (string): The Parquet engine to use. Defaults to 'pyarrow'.
    
    Returns:
        - df (pd.DataFrame): The data read from the Parquet file.
        - elapsed (float): Time in seconds it took to read the file.
    """
    print(f"\nReading with {engine} engine...")
    start_time = time.time()
    df = pd.read_parquet(filepath, engine=engine)
    elapsed = time.time() - start_time
    print(f"✓ Completed in {elapsed:.3f} seconds")
    print(f"  Rows: {len(df)}, Columns: {len(df.columns)}")
    return df, elapsed


def main():
    print("=" * 60)
    print("Parquet Hello World: PyArrow vs FastParquet")
    print("=" * 60)
    
    # Create output directory if it doesn't exist
    output_dir = 'output'
    os.makedirs(output_dir, exist_ok=True)
    
    # Create sample data
    print("\nCreating sample data...")
    df = create_sample_data(100000)
    print(f"✓ Created DataFrame: {len(df)} rows, {len(df.columns)} columns")
    print(f"  Columns: {', '.join(df.columns)}")
    
    # Write with PyArrow
    pyarrow_file = os.path.join(output_dir, 'data_pyarrow.parquet')
    pyarrow_time, pyarrow_size = write_with_pyarrow(df, pyarrow_file)
    
    # Write with FastParquet
    fastparquet_file = os.path.join(output_dir, 'data_fastparquet.parquet')
    try:
        fastparquet_time, fastparquet_size = write_with_fastparquet(df, fastparquet_file)
    except ImportError:
        print("\n⚠ FastParquet not installed. Skipping FastParquet demonstration.")
        fastparquet_time, fastparquet_size = None, None
    
    # Read back with PyArrow
    _, pyarrow_read_time = read_parquet(pyarrow_file, engine='pyarrow')
    
    # Read back with FastParquet
    fastparquet_read_time = None
    if fastparquet_time is not None:
        try:
            _, fastparquet_read_time = read_parquet(fastparquet_file, engine='fastparquet')
        except Exception as e:
            print(f"⚠ Could not read with FastParquet: {e}")
    
    # Performance summary
    print("\n" + "=" * 60)
    print("Performance Summary")
    print("=" * 60)
    print(f"\nPyArrow:")
    print(f"  Write time: {pyarrow_time:.3f}s")
    print(f"  File size: {pyarrow_size:.2f} MB")
    
    if fastparquet_time is not None:
        print(f"\nFastParquet:")
        print(f"  Write time: {fastparquet_time:.3f}s")
        print(f"  File size: {fastparquet_size:.2f} MB")
        speedup = fastparquet_time / pyarrow_time
        size_diff = ((fastparquet_size - pyarrow_size) / pyarrow_size) * 100
        print(f"\nWrite Comparison:")
        print(f"  Speedup (FastParquet vs PyArrow): {speedup:.2f}x")
        print(f"  Size difference: {size_diff:+.1f}%")
    
    print(f"\nRead times:")
    print(f"  PyArrow: {pyarrow_read_time:.3f}s")
    if fastparquet_read_time is not None:
        print(f"  FastParquet: {fastparquet_read_time:.3f}s")
        speedup = fastparquet_read_time / pyarrow_read_time
        print(f"  Speedup (FastParquet vs PyArrow): {speedup:.2f}x")
    print("=" * 60)


if __name__ == '__main__':
    main()
