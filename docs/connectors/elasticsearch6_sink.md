# ElasticSearch Sink Connector

Astatine support to sink data to ElasticSearch(version 6.x) .

```sql
CREATE TABLE source (
   name  STRING,
   score INT
) <@template.table_socket_source hostname = 'localhost' />

CREATE TABLE sink_elasticsearch (
    name            STRING,
    score           INT,
    PRIMARY KEY (name) NOT ENFORCED
) <@template.table_astatine_elasticsearch6_sink
    routing = '{name}'
    function\.id = 'default'
    document\-type = 'doc'
    index = 'test_index' />

INSERT INTO sink_elasticsearch
SELECT name,score
FROM source;
```

### Connector Options

The Astatine ElasticSearch Sink Connector is based on the Flink ElasticSearch6 Sink Connector, so all options can be found in [Flink Elasticsearch SQL Connector](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/elasticsearch/)

The Astatine ElasticSearch Sink Connector also supports the following options:

| Option      | Type   | Default   | Description                                                                                                                                                                                                                                                                  |
|-------------|--------|-----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| routing     | String | (none)    | Specify the elasticsearch routing field                                                                                                                                                                                                                                      |
| function.id | String | (default) | Specify the sink function implementing the interface of [RowElasticsearchSinkFunction](../../astatine-connectors/astatine-connector-elasticsearch6/src/main/java/name/zicat/astatine/connector/elasticsearch6/RowElasticsearchSinkFunction.java) and register it by Java Spi |

## Community VS Astatine

Astatine ElasticSearch Sink Connector is the superset of Community ElasticSearch Sink Connector. Astatine ElasticSearch Sink Connector has 2 Built-in functions.

- `function.id = 'default'` is the default value of function.id. It means using Community and supporting all options.
- `function.id = 'append_only_function'` means using append-type-document api even if the sink table has primary key and also supports all options.
  
  The append-type-document api is more efficient than the upsert-type-document api. Highly recommend it if the sink schema with primary key is completely consistent with the ElasticSearch schema.    
    