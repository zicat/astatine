# Astatine Overview

Astatine is a streaming SQL development platform based on [Flink-1.17.1](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/overview/).

Go to [learn how to run a simple streaming sql job on docker in local environment](docs/your_first_simple_job.md)
to start your first streaming sql.

# Astatine Extensions

- [Sql Template](docs/template.md)

- Formats

    - [Protobuf](docs/formats/format_protobuf.md)

- Source & Sink

    - [Kafka Source Sink](docs/connectors/kafka_source_sink.md)
    - [Http Sink](docs/connectors/http_sink.md)
    - [Socket Source](docs/connectors/socket_source.md)
    - [ElasticSearch6 Sink](docs/connectors/elasticsearch6_sink.md)
    - [HBase Sink](docs/connectors/hbase_sink.md)
    - [Paimon Source Sink](docs/connectors/paimon_source_sink.md)

- Functions

  - [Develop Functions](docs/udf_develop.md)
  - [Exist Functions](docs/udf.md)
  
- [Streaming SQL](docs/streaming_sql.md)

    - [Field Value Watch Changed Emitter Operator](docs/streaming_operators/field_value_watch_changed_emitter_operator.md)
    - [Eventtime Temporal Join Operator](docs/streaming_operators/streaming_eventtime_temporal_join_operator.md)
    - [Row 2 Pojo Operator](docs/streaming_operators/row_2_pojo_operator.md)

# Useful Links

- [Flink 1.17.1 Query SQL Doc](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/overview/)
- [Flink 1.17.1 Configuration](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/deployment/config/)
- [Flink Event Time and Watermark](docs/useful_links/flink_event_time_and_wk.md)
- [Temporal Join VS Interval Join in Streaming Job](docs/useful_links/how_to_choose_a_suitable_join_type.md)
- [How to Optimize Multi-Metrics in Same Dimensions With Flink SQL](docs/useful_links/how_to_optimize_multi_metrics_in_same_dimensions_with_flink_sql.md)