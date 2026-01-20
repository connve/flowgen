# Flowgen Examples

This directory contains example flows demonstrating various flowgen features and use cases.

## Directory Structure

- **`data/`** - Synthetic test data for examples
- **`object-store/`** - Examples for reading and writing files
- **`cloudflare/`** - Examples for Cloudflare integrations
- **`salesforce/`** - Examples for Salesforce integrations

## Test Data

The `data/` directory contains synthetic datasets generated for testing and demonstration purposes:

- **`orders.csv`** - 10,000 synthetic order records with fields: id, order_number, order_date, payment_type, amount, customer_id, status

All data in this directory is randomly generated and does not represent real customer or business data.

## Running Examples

Examples reference paths like `file:///flowgen/examples/data/orders.csv`. Update these paths to match your deployment location.
