# Serialized Writing

Avro writes data record by record, making it ideal for logging and streaming systems where new records arrive continuously.

Unlike Parquet, which requires all data to be available before writing, Avro allows appending individual records to an existing file without rewriting it.

## What This Example Does

- Writes an initial batch of orders to a new Avro file
- Appends a single new order to the existing file
- Reads all orders back to verify correctness

## Running the Example

Activate the virtual environment from the root of the repository:

    source venv/bin/activate
    pip install fastavro

Run the script:

    python3 write_orders.py