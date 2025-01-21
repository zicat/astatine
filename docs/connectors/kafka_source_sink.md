# Kafka Source Sink Connector

Astatine support to source and sink data to kafka.

```sql
CREATE TABLE kafka_source (
   name            STRING,
   score           INT,
   ts              BIGINT,
   event_time AS to_timestamp3(ts)
) <@template.table_kafka_source 
    topic = 'test_topic'
    ... />

CREATE TABLE kafka_sink (
   name            STRING,
   score           INT,
   ts              BIGINT
) <@template.table_kafka_sink
    topic = 'test_topic_output'
    format = 'protobuf'
    protobuf\.message\-class\-name = 'name.zicat.astatine.formats.protobuf.Test$NameScoreTs'
    protobuf\.ignore\-parse\-errors = 'true'
    .../>
```

## Connector Options

The Kafka Connector is used Flink Kafka Sink Connector, so all options can be found in [Flink Kafka SQL Connector](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/kafka/)