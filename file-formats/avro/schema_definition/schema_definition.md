# Schema Definition

A schema is a contract that describes the structure of your data. It defines what fields exist, what types they are, and which fields are optional.

In Avro, schemas are written in JSON and can be defined in two ways: stored in a dedicated `.avsc` file, or written inline as a Python dictionary directly in your code.

## The Order Schema

The schema for this example represents an e-commerce order with the following fields:

- `order_id`: a unique integer identifier for the order
- `customer_name`: the name of the customer as a string
- `product`: the name of the purchased product as a string
- `quantity`: the number of units ordered as an integer
- `price`: the price of the order as a double
- `status`: an optional string representing the current state of the order, defaults to null if not provided