# Databricks Stream Processing

A hands-on Apache Spark Structured Streaming repository covering foundational streaming concepts, Kafka integration, Delta Lake ingestion, medallion architecture, incremental aggregations, and event-time windowing.

The repository contains three learning tracks:

- **`Notebooks/`** — progressive Spark Structured Streaming exercises and test suites.
- **`SBIT/`** — a larger end-to-end streaming project migrated to Databricks Unity Catalog Volumes, Delta tables, and Confluent Cloud Kafka.
- **`InvoiceProducer/`** — a Kafka producer notebook for publishing invoice events to the `invoices` topic.

> **Runtime note:** The project has been adapted for Databricks Free Edition/serverless in several places. Serverless has feature and execution-mode constraints. In particular, the SBIT project uses `AvailableNow` for finite incremental streaming runs. Review the configuration and run the smoke tests in your own workspace before treating the repository as production-ready.

## Repository structure

```text
dbx-stream-processing-main/
├── InvoiceProducer/
│   ├── data/
│   │   └── invoices.json
│   └── invoice_producer.ipynb
├── Notebooks/
│   ├── 01-streaming-word-count.ipynb
│   ├── 02-word-count-test-suite.ipynb
│   ├── 03-invoice-stream.ipynb
│   ├── 04-invoice-stream-test-suite.ipynb
│   ├── 05-streaming-batch.ipynb
│   ├── 06-streaming-batch-test-suite.ipynb
│   ├── 07-medallion-approach.ipynb
│   ├── 08-medallion-approach-test-suite.ipynb
│   ├── 09-kafka-batch-consumer.ipynb
│   ├── 10-kafka-to-bronze.ipynb
│   ├── 11-kafka-to-bronze-test-suite.ipynb
│   ├── 12-idempotent-kafka-to-bronze.ipynb
│   ├── 13-idempotent-kafka-to-bronze-test-suite.ipynb
│   ├── 14-kafka-producer.ipynb
│   ├── 15-kafka-producer-test-suite.ipynb
│   ├── 16-streaming-aggregates.ipynb
│   ├── 17-streaming-aggregates-test-suite.ipynb
│   ├── 18-streaming-incremental-aggregates.ipynb
│   ├── 19-streaming-incremental-aggregates-test-suite.ipynb
│   ├── 20-streaming-unbounded-incremental-aggregates.ipynb
│   ├── 21-streaming-unbounded-incremental-aggregates-test-suite.ipynb
│   ├── 22-tumbling-time-window.ipynb
│   ├── 23-tumbling-time-window-test-suite.ipynb
│   ├── 24-sliding-window.ipynb
│   └── 25-sliding-window-test-suite.ipynb
├── SBIT/
│   ├── 00-scratchpad.ipynb
│   ├── 01-config.ipynb
│   ├── 02-setup.ipynb
│   ├── 03-history-loader.ipynb
│   ├── 04-bronze.ipynb
│   ├── 05-silver.ipynb
│   ├── 06-gold.ipynb
│   ├── 07-run.ipynb
│   ├── 08-batch-test.ipynb
│   ├── 09-stream-test.ipynb
│   └── 10-producer.ipynb
├── data/
│   ├── invoices/
│   ├── invoices_archive/
│   └── text/
└── datasets/
    ├── invoices/
    ├── results/
    └── text/
```

## Learning path

A sensible order for the standalone exercises is:

1. Word count: batch vs. streaming.
2. Invoice stream processing.
3. Streaming and batch processing patterns.
4. Medallion architecture.
5. Kafka batch consumption and Kafka-to-Bronze ingestion.
6. Idempotent Kafka ingestion.
7. Kafka publishing.
8. Streaming aggregations and incremental aggregation strategies.
9. Tumbling and sliding event-time windows.

The test-suite notebooks generally follow their corresponding implementation notebook. Run the implementation first, then its test suite.

## SBIT end-to-end project

The SBIT folder contains a separate pipeline-oriented project. Its intended execution sequence is:

1. `01-config` — central configuration and secret retrieval.
2. `02-setup` — create the project schema/tables and required storage directories.
3. `03-history-loader` — load historical/reference data.
4. `04-bronze` — ingest source data into Bronze.
5. `05-silver` — apply cleansing and upsert/CDC-style transformations.
6. `06-gold` — produce aggregated/business-facing outputs.
7. `07-run` — orchestrate the pipeline.
8. `08-batch-test` — validate batch processing.
9. `09-stream-test` — validate incremental streaming scenarios.
10. `10-producer` — publish the sample event datasets to Kafka and copy file-based inputs to the landing zone.

`00-scratchpad` is an interactive notebook for invoking setup, validation, cleanup, and pipeline methods while debugging. It is not a replacement for the individual implementation notebooks.

See [`SBIT/README.md`](SBIT/README.md) for setup details and operational cautions.

## Prerequisites

- A Databricks workspace with permission to use a compute resource compatible with the notebooks.
- Unity Catalog enabled for the SBIT configuration.
- A Unity Catalog Volume for input data and streaming checkpoints.
- Confluent Cloud cluster and Kafka topics for Kafka exercises.
- Databricks secret scope containing Kafka credentials.
- Spark Kafka connector availability on the selected compute for notebooks using `.format("kafka")`.

## Configuration

The SBIT configuration notebook currently uses these defaults:

| Setting | Default |
|---|---|
| Catalog | `spark_stream` |
| Project schema | `sbit_db` |
| Volume catalog | `spark_stream` |
| Volume schema | `default` |
| Volume name | `streaming_data` |
| Volume path | `/Volumes/spark_stream/default/streaming_data` |
| Secret scope | `kafka-secrets` |

The secret scope is expected to contain:

- `bootstrap-server`
- `cluster-api-key`
- `cluster-api-secret`

Create these secrets in Databricks. Do not hardcode credentials in notebooks, Git, README files, or logs.

The project uses the following logical directories under the Volume:

```text
/Volumes/spark_stream/default/streaming_data/
├── test_data/
├── raw/
└── checkpoints/
```

The exact files and subdirectories required depend on the exercise being run. Upload the repository's sample data to the expected Volume paths before running the relevant notebooks.

## Kafka topics

The SBIT producer and ingestion notebooks use these topics:

- `user_info`
- `workout`
- `bpm`

The standalone invoice producer/consumer exercises use:

- `invoices`

Create the topics in Confluent Cloud before running the relevant producer or consumer notebooks. Confirm that the topic names in the notebooks match the topics in the cluster.

## Running on Databricks

1. Clone or import the repository into your Databricks workspace.
2. Create the Unity Catalog catalog/schema/Volume required by your chosen track, or update the configuration to match existing resources.
3. Upload the relevant sample data to the expected paths.
4. Create the Databricks secret scope and secret keys.
5. Create the Kafka topics.
6. Open the implementation notebook and run it before its test suite.
7. Inspect output tables, Kafka offsets, and checkpoint state before proceeding to the next stage.

For the SBIT project, start with `SBIT/00-scratchpad` or follow the explicit sequence in `SBIT/README.md`.

## Checkpoints and reruns

Structured Streaming checkpoints store progress and state. Treat them as durable pipeline state:

- Reuse the same checkpoint when continuing the same logical stream.
- Use a new checkpoint only when intentionally starting a new independent stream or replay.
- Deleting a checkpoint does **not** delete Kafka messages.
- Resetting a checkpoint can cause previously consumed Kafka records to be read again, depending on the configured starting offsets.
- A batch Kafka write does not gain streaming checkpoint-based exactly-once behavior simply because a checkpoint path exists elsewhere in the project.
- Do not delete checkpoint directories while a query is active.

For repeatable tests, use isolated test topics or deliberately reset both the relevant source data and the target/checkpoint state.

## Important implementation notes

- `AvailableNow` processes the data currently available and then terminates. It is useful for finite incremental runs; it is not a continuously running streaming trigger.
- Kafka's `startingOffsets` and `startingTimestamp` options apply when establishing a new streaming query/checkpoint. An existing checkpoint normally resumes from its recorded offsets.
- A Kafka batch read cannot use `startingOffsets = latest`; use a valid batch offset range or a streaming read for that use case.
- Kafka record timestamps, event timestamps inside payloads, and source-file timestamps are distinct concepts. Preserve event time explicitly in the payload if downstream consumers need the original event time.
- Validate record counts and offsets rather than relying only on producer-side success messages.
- Unity Catalog object names use catalog/schema/table structure. Volume paths are separate from table namespaces.

## Troubleshooting checklist

| Symptom | What to check |
|---|---|
| `UC_INVALID_NAMESPACE` | Verify catalog and schema variables separately; check that a three-part table name is not being passed where a schema name is expected. |
| Kafka topic appears empty | Confirm the producer and consumer use the same cluster and topic; inspect partition high-watermarks and retention settings. |
| Producer reports delivery but consumer sees zero | Check consumer offset position, committed offsets, `startingOffsets`, topic partitions, and whether the consumer is reading from the beginning or end. |
| Streaming query replays records | Inspect checkpoint path and source offsets; do not casually delete checkpoints. |
| `ProcessingTime` unsupported | On the relevant serverless environment, use a supported finite trigger such as `AvailableNow` where appropriate. |
| Kafka source/sink cannot be found | Verify the Spark Kafka connector is available on the selected compute. Installing the Python `confluent-kafka` package does not install Spark's Kafka connector. |
| No rows in Bronze | Verify Kafka has records, the source subscription is correct, the query completed, and the target table/catalog/schema are correct. |

## Security

Never commit API keys, passwords, JAAS strings containing credentials, or personal access tokens. Store credentials in Databricks secrets and restrict access to the secret scope. Rotate any credential that has been exposed in source control or screenshots.

## Project status

This repository is a learning and practice project. Notebook presence does not imply that every notebook has been executed successfully in every Databricks runtime. Validate each track end-to-end in the target workspace before using it for production workloads.
