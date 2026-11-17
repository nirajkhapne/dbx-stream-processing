# Structured Streaming Notebooks

This folder contains progressive Spark Structured Streaming exercises and their test suites.

## Notebook index

| Notebook | Focus |
|---|---|
| `01-streaming-word-count` | Batch and streaming word-count implementations |
| `02-word-count-test-suite` | Tests for word-count behavior |
| `03-invoice-stream` | Invoice streaming transformations |
| `04-invoice-stream-test-suite` | Tests for invoice stream processing |
| `05-streaming-batch` | Batch-oriented processing of invoice data |
| `06-streaming-batch-test-suite` | Tests for the batch processing exercise |
| `07-medallion-approach` | Bronze/Silver/Gold processing pattern |
| `08-medallion-approach-test-suite` | Tests for the medallion exercise |
| `09-kafka-batch-consumer` | Read Kafka records with Spark's Kafka source in batch mode |
| `10-kafka-to-bronze` | Ingest Kafka events into a Bronze Delta table |
| `11-kafka-to-bronze-test-suite` | Tests for Kafka-to-Bronze ingestion |
| `12-idempotent-kafka-to-bronze` | Idempotent Kafka-to-Bronze ingestion pattern |
| `13-idempotent-kafka-to-bronze-test-suite` | Tests for idempotent ingestion |
| `14-kafka-producer` | Kafka publishing exercise |
| `15-kafka-producer-test-suite` | Tests for the producer exercise |
| `16-streaming-aggregates` | Streaming aggregation |
| `17-streaming-aggregates-test-suite` | Tests for streaming aggregation |
| `18-streaming-incremental-aggregates` | Incremental aggregation |
| `19-streaming-incremental-aggregates-test-suite` | Tests for incremental aggregation |
| `20-streaming-unbounded-incremental-aggregates` | Unbounded incremental aggregation pattern |
| `21-streaming-unbounded-incremental-aggregates-test-suite` | Tests for unbounded incremental aggregation |
| `22-tumbling-time-window` | Tumbling event-time windows |
| `23-tumbling-time-window-test-suite` | Tests for tumbling windows |
| `24-sliding-window` | Sliding event-time windows |
| `25-sliding-window-test-suite` | Tests for sliding windows |

## How to run

For each topic, run the implementation notebook first, then the matching test-suite notebook. For example:

1. Run `01-streaming-word-count`.
2. Run `02-word-count-test-suite`.

Continue in numerical order, ensuring that the required input files and output/checkpoint locations exist.

## Data

The repository includes text and invoice sample datasets under the root `data/` and `datasets/` directories. Some notebooks resolve data paths relative to the workspace notebook location. If a path does not resolve after importing the repository, update the notebook's base path or copy the sample files to a Unity Catalog Volume and update the configured path.

## Kafka requirements

Kafka exercises use the Spark Kafka data source/sink (`format("kafka")`). This requires the Spark Kafka connector on the selected compute. The Python package `confluent-kafka` is a separate client library and is not a substitute for the Spark connector.

The notebooks retrieve Kafka connection settings from the Databricks secret scope `kafka-secrets`. Expected keys:

- `bootstrap-server`
- `cluster-api-key`
- `cluster-api-secret`

Create the topics used by the notebook before running it. The invoice Kafka exercises use the `invoices` topic.

## Checkpoint discipline

Use a stable checkpoint path for a continuing stream. Use a new path for a deliberate replay or independent test run. Deleting a checkpoint resets Spark's recorded source progress; it does not erase the Kafka topic. Verify source offsets and target-table counts after each run.
