# Astatine Overview

Astatine is a streaming SQL development platform based on [Flink-1.17.1](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/overview/).

Go to [learn how to run a simple streaming sql job on docker in local environment](docs/your_first_simple_job.md)
to start your first streaming sql.

# Astatine Extensions

- Source & Sink

    - [Http Sink](docs/connectors/http_sink.md)
    - [Socket Source](docs/connectors/socket_source.md)

- [User Defined Functions(UDF)](docs/udf.md)

- [Astatine Streaming SQL](docs/streaming_sql.md)

    - [Field Value Watch Changed Emitter Operator](docs/streaming_operators/field_value_watch_changed_emitter_operator.md)
    - [Eventtime Temporal Join Operator](docs/streaming_operators/streaming_eventtime_temporal_join_operator.md)
    - [Row 2 Pojo Operator](docs/streaming_operators/row_2_pojo_operator.md)