# Compression Toggling

## Overview
Avro supports multiple compression codecs inside Object Container Files. The codec is stored in the file metadata (`avro.codec`) and determines how data blocks are compressed. Choosing the right codec depends on the tradeoff between speed, storage size, and compatibility.

## What You'll Learn
- The available Avro compression codecs
- The differences between null, deflate, snappy, and zstandard
- When to use each codec
- The impact of compression on performance and storage

## Concepts

### null
No compression is applied. Data is written as-is inside the container file.
- Fastest write and read
- No storage optimization
- Useful for debugging and testing

### deflate
Standard compression based on zlib and part of the official Avro specification.
- Good compression ratio
- Slower than snappy
- Fully supported across all Avro implementations

### snappy
Optional codec in Avro, designed for high-speed compression.
- Very fast compression and decompression
- Lower compression ratio than deflate
- Commonly used in streaming systems like Kafka

### zstandard (zstd)
Modern compression algorithm not included in the official Avro specification.
- High compression ratio with good speed
- Supported by some modern platforms
- Not guaranteed to work across all systems

## Key Topics

### Codec Selection
The codec is defined when writing the file and stored in metadata:

```python
fastavro.writer(f, schema, orders, codec="snappy")
```

### Compatibility
Only `null` and `deflate` are guaranteed by the Avro specification. `snappy` is widely supported but still optional. `zstandard` depends on the implementation and requires the `zstandard` Python package.

### Real-World Results
Running the recipe against 10,000 order records produces the following:

```
Codec: null         Size: 320794 bytes
Codec: deflate      Size: 102788 bytes
Codec: snappy       Size: 146793 bytes
Codec: zstandard    Size: 107029 bytes
```

`deflate` and `zstandard` achieve the best compression ratios, reducing file size by roughly 68%. `snappy` trades some compression ratio for faster read and write speeds.

## Advanced Tips
- Use `snappy` for streaming pipelines where low latency matters
- Use `deflate` for storage optimization with broad compatibility
- Use `null` for debugging and local testing
- Use `zstandard` only if all systems in your pipeline support it

## Important Note
`zstandard` is not part of the official Avro specification. It is supported by some modern data platforms, but using it may reduce portability between systems. It also requires installing the `zstandard` Python package separately.
