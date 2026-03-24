# Schema Definition

## Overview
A schema is a contract that describes the structure of your data. In Avro, schemas are written in JSON and can be defined in two ways: stored in a dedicated `.avsc` file, or written inline as a Python dictionary directly in your code.

## What You'll Learn
- How to define an Avro schema in a `.avsc` file
- How to define a schema inline in Python
- The difference between the two approaches and when to use each

## Concepts

### File-Based Schema
Storing the schema in a `.avsc` file keeps it separate from your code, making it reusable across multiple scripts and easier to version alongside your data.

### Inline Schema
Defining the schema as a Python dictionary is useful for quick scripts or when the schema is tightly coupled to the code and unlikely to be reused elsewhere.

## Key Topics

### Schema Structure
Every Avro schema has three required fields:
- `type`: always `record` for structured data
- `name`: the name of the record, equivalent to a table name
- `namespace`: a prefix that prevents name collisions across schemas

### Field Types
Avro supports primitive types such as `int`, `long`, `float`, `double`, `string`, and `boolean`, as well as complex types like arrays, maps, and unions. A union of `null` and another type is how optional fields are represented.

## Advanced Tips
- Use file-based schemas when the schema is shared across multiple scripts or services
- Use inline schemas for quick prototyping or single-use scripts
- Always version your `.avsc` files alongside your data to track schema changes over time
