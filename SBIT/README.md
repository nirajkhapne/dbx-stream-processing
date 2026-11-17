# SBIT — Streaming Business Integration Project

This folder contains the end-to-end streaming project organized into configuration, setup, history loading, Bronze, Silver, Gold, orchestration, tests, and Kafka data production.

## Notebook map

| Notebook | Responsibility |
|---|---|
| `00-scratchpad` | Interactive execution and debugging of project methods |
| `01-config` | Catalog, schema, Volume paths, trigger settings, and Kafka secrets |
| `02-setup` | Project schema, tables, and storage setup/validation/cleanup |
| `03-history-loader` | Load historical/reference data such as the date lookup |
| `04-bronze` | Ingest source files and Kafka data into Bronze |
| `05-silver` | Clean, transform, and upsert Silver data |
| `06-gold` | Build aggregated/business-facing Gold outputs |
| `07-run` | Pipeline orchestration |
| `08-batch-test` | Batch-path test execution |
| `09-stream-test` | Incremental/streaming test execution |
| `10-producer` | Copy file-based inputs and publish event JSON files to Kafka |

## Unity Catalog configuration

The current `01-config` notebook defines:

```python
catalog = "spark_stream"
db_name = "sbit_db"

volume_catalog = "spark_stream"
volume_schema = "default"
volume_name = "streaming_data"
```

The Volume path is:

```text
/Volumes/spark_stream/default/streaming_data
```

The project schema for managed tables is separate:

```text
spark_stream.sbit_db
```

Do not confuse the Volume's schema (`default`) with the project table schema (`sbit_db`). A Volume path and a fully qualified table name are different kinds of identifiers.

Expected logical Volume layout:

```text
/Volumes/spark_stream/default/streaming_data/
├── test_data/
├── raw/
└── checkpoints/
```

Upload the SBIT test data into `test_data/` and ensure the setup notebook creates the required landing-zone and checkpoint directories.

## Kafka configuration

The config notebook retrieves credentials from Databricks secret scope `kafka-secrets`:

| Secret key | Meaning |
|---|---|
| `bootstrap-server` | Confluent Kafka bootstrap endpoint |
| `cluster-api-key` | Kafka API key |
| `cluster-api-secret` | Kafka API secret |

The producer publishes to:

- `user_info`
- `workout`
- `bpm`

Create these topics in the target Confluent cluster first. Verify that the producer and Spark Kafka consumer use the same bootstrap server and credentials.

## Recommended execution order

### Initial setup

1. Run `01-config`.
2. Run `02-setup`.
3. Run `03-history-loader`.
4. Validate that the project schema, required tables, Volume paths, and reference data exist.

### Initial data load

5. Run `10-producer` with dataset 1.
6. Run `04-bronze` to ingest the Kafka/file inputs.
7. Run `05-silver`.
8. Run `06-gold`.
9. Validate the Bronze, Silver, and Gold outputs.

### Incremental run

10. Run `10-producer` with dataset 2.
11. Re-run Bronze, Silver, and Gold in dependency order, retaining the existing checkpoints for continuing streams.
12. Run `09-stream-test` to validate incremental behavior.

`07-run` provides orchestration, while `08-batch-test` and `09-stream-test` provide test workflows. Review their cleanup behavior before executing them: cleanup may drop tables or delete checkpoint/data directories.

## Producer input files

The producer expects these files under `test_data/`:

```text
1-registered_users_<set_num>.csv
5-gym_logins_<set_num>.csv
2-user_info_<set_num>.json
4-workout_<set_num>.json
3-bpm_<set_num>.json
```

The JSON event files contain records with fields such as `key`, `value`, `topic`, `partition`, `offset`, and `timestamp`. The producer should publish the message key and serialized value to the selected destination topic. Kafka assigns the destination partition and offset; source `partition` and `offset` fields are metadata from the input record, not destination coordinates.

Confirm the actual file encoding and JSON shape before changing schemas. If `value` is already a JSON string, do not JSON-encode it a second time.

## Serverless / Free Edition considerations

- Prefer finite `AvailableNow` runs for supported incremental streaming workloads.
- Do not assume `ProcessingTime` triggers, SparkContext APIs, JVM access, or all cluster-level Spark configurations are available on serverless.
- Spark Kafka source/sink support must be verified on the selected compute.
- A batch Kafka sink write and a Structured Streaming Kafka sink are different execution models. Use batch writes for static files that should be published as a one-time batch; use Structured Streaming when the input is genuinely a streaming source.
- Preserve checkpoints between incremental runs. Deleting a checkpoint can replay Kafka records.
- Validate Kafka offsets and target-table counts independently; a completed producer call alone is not proof that the downstream Bronze query consumed the records.

## Validation checklist

- [ ] Catalog `spark_stream` exists and is accessible.
- [ ] Project schema `spark_stream.sbit_db` exists.
- [ ] Volume `spark_stream.default.streaming_data` exists.
- [ ] Required `test_data`, `raw`, and `checkpoints` paths exist.
- [ ] Kafka secret scope and keys are configured.
- [ ] Kafka topics `user_info`, `workout`, and `bpm` exist in the correct cluster.
- [ ] Dataset 1 records are visible in Kafka partition offsets.
- [ ] Bronze tables contain the expected records.
- [ ] Silver upserts and deduplication behave as intended.
- [ ] Gold aggregates match expected results.
- [ ] Dataset 2 is processed incrementally without unintended replay or duplication.

## Operational warning

Do not run `cleanup()` casually. Inspect its implementation first. Dropping tables, deleting landing-zone files, or deleting checkpoints can invalidate the current test state. Use a clean, isolated test setup when you need a full replay.
