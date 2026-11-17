# InvoiceProducer

This folder contains a Databricks notebook that publishes invoice events from `data/invoices.json` to the Confluent Kafka topic `invoices`.

## Notebook

- `invoice_producer.ipynb` — defines `InvoiceProducer`, configures a Kafka producer, reads invoice JSON lines, and publishes a selected number of events.

## Input

The notebook expects:

```text
InvoiceProducer/data/invoices.json
```

The input is read line by line as JSON. The producer uses `StoreID` as the Kafka message key and serializes the invoice object as the message value.

## Kafka setup

Create the `invoices` topic in Confluent Cloud. Configure a Databricks secret scope named `kafka-secrets` with:

- `bootstrap-server`
- `cluster-api-key`
- `cluster-api-secret`

The notebook uses SASL_SSL with the PLAIN mechanism. Do not put credentials directly into the notebook or repository.

## Run

Import the notebook into Databricks, ensure the input path resolves in the workspace, configure the secrets, then execute the notebook. The current notebook's `start()` method publishes up to 20 invoices.

## Notes

- `flush()` waits for outstanding messages to be delivered or fail.
- The delivery callback reports message delivery status.
- Publishing the same input again creates another set of Kafka records; the producer does not implement deduplication.
- The local relative path used by the notebook may need adjustment after importing it into a different workspace location.
