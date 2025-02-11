# Astatine Streaming SQL

Astatine Streaming SQL is a superset of Flink SQL fully compatible with Flink SQL supporting orchestrating Flink Streaming Jobs.

This page lists all the supported statements supported in Astatine Streaming SQL for now:

- CREATE STREAM identity FROM identity2 transforms
- CREATE VIEW identity FROM identity2 transforms
- PRINT FROM identity

## CREATE STREAM FROM

`CREATE STREAM identity FROM identity2` support to create a stream from other streams, tables or views.

``` sql
-- flink sql
CREATE TABLE source (
  name STRING ,
  score INT
) <@template.table_socket_source hostname = 'localhost' />

-- astatine sql, create stream from table/view
CREATE STREAM stream_source 
FROM source
MAP WITH (
    'identity' = 'row_2_pojo',
    'mapping.class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore'
);

-- astatine sql, create stream from other stream
CREATE STREAM stream_source_double 
FROM stream_source
MAP WITH (
    'identity' = 'score_double'
);

PRINT FROM stream_source_double;
```

If you want to create a stream from a table or view with type of RowData, you can use the following sql:

``` sql
CREATE TABLE source (
  name STRING ,
  score INT
) <@template.table_socket_source hostname = 'localhost' />

CREATE STREAM stream_source
FROM source WITH (
  'product.type' = 'RowData'
);

PRINT FROM stream_source;
```

## CREATE VIEW FROM

`CREATE VIEW identity FROM identity2` support to create a view from stream.

```sql
CREATE TABLE source (
  name              STRING,
  score             INT,
  ts                TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) <@template.table_socket_source hostname = 'localhost' />

CREATE STREAM stream_source
FROM source WITH (
  'product.type' = 'RowData'
);

CREATE VIEW view_source WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM stream_source;

PRINT FROM view_source;
```

Creating view support to set watermark expression with `expression.watermark` in `WITH` clause.

The `SOURCE_WATERMARK()` is a built-in function to get the watermark of the source stream.

## PRINT FROM

`PRINT FROM identity` support to print the result of the stream or view or table without define print-sink table.

```sql
CREATE TABLE source (
  name              STRING,
  score             INT,
  ts                TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) <@template.table_socket_source hostname = 'localhost' />

PRINT FROM source;
```

## Transforms

Astatine use `Transforms` to organize the operators of stream processing.

The syntax of Transforms is as follows:

```sql
trasnaform_id WITH (
    'identity' = 'identity_name',
    ......
)
```

The following is the list of supported transforms:

- MAP WITH

  Define a [map function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/MapFunctionFactory.java) registering by spi to map the input stream.

- FLAT MAP WITH

  Define
  a [flat map function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/FlatMapFunctionFactory.java)
  registering by spi to flat map the input stream.

- UNION identity2

  The union operator is used to combine two streams into a single stream.

- REDUCE

  Define
  a [reduce function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/ReduceFunctionFactory.java)
  registering by spi to reduce the keyed input stream.

- JOIN identity2 WITH

  Define
  a [join function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/JoinFunctionFactory.java)
  registering by spi to transform the JoinedStreams.

- INTERVAL JOIN identity2 WITH

  Define
  an [interval join function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/IntervalJoinFunctionFactory.java)
  registering by spi to transform the IntervalJoinedStreams.

- CONNECT identity2 WITH

  Define
  a [connect function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/ConnectFunctionFactory.java)
  registering by spi to transform the ConnectedStreams.

- FILTER

  Define
  a [filter function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/FilterFunctionFactory.java)
  registering by spi to filter the input stream.

- KEY BY WITH

  Define
  a [key by function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/KeyByFunctionFactory.java)
  registering by spi to key by the input stream.

- PROCESS WITH

  Define
  a [process function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/ProcessFunctionFactory.java)
  registering by spi to process the input stream.

  Define
  a [keyed process function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/KeyedProcessFunctionFactory.java)
  registering by spi to process the keyed input stream.

- FORWARD

  The forward operator is used to forward the input stream to the output stream.

If the transforms above does not meet the requirements, you can use common transform to define your own operator.

- TRANSFORM [identity2] WITH

  Define
  a [transform function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/OneTransformFunctionFactory.java)
  registering by spi to transform the input stream to other stream.

  Define
  a [two transform function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/TwoTransformFunctionFactory.java)
  registering by spi to transform the 2 input streams to other stream if having identity2.

  Define
  a [keyed transform function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/OneKeyedTransformFunctionFactory.java)
  registering by spi to transform the keyed input stream to other stream.

  Define
  a [keyed two transform function factory](../astatine-streaming-sql/astatine-streaming-sql-parser/src/main/java/name/zicat/astatine/streaming/sql/parser/function/TwoKeyedTransformFunctionFactory.java)
  registering by spi to transform the 2 keyed input streams to other stream if having identity2.

## Example of mixed usage of Flink SQL and Astatine Streaming SQL
```sql
CREATE TABLE source (
  name              STRING,
  score             INT,
  ts                TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
) <@template.table_socket_source hostname = 'localhost' />

-- create a view with watermark from stream sql operators
-- the whole operators include table->streaming->key by->process by field_value_watch_changed_emitter->view
CREATE VIEW view_stream_demo
WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
  'product.type' = 'RowData'
) KEY BY WITH(
    'identity' = 'key_by_rowdata',
    'fields' = 'name'
) PROCESS WITH (
    'identity' = 'field_value_watch_changed_emitter',
    'watch.field' = 'score',
    'eventtime' = 'ts'
);

-- create a view with tumbling window
CREATE VIEW view_avg_changed_result_score AS
SELECT name, SUM(score) AS avg_score, TUMBLE_END(ts, INTERVAL '1' MINUTE) AS ts
FROM view_stream_demo
GROUP BY name, TUMBLE(ts, INTERVAL '1' MINUTE);

-- print
PRINT FROM view_avg_changed_result_score;
```

Input:

```shell
$ nc -l 9999
{"name":"n1","score":90,"ts":"2025-01-22 12:00:10"}
{"name":"n1","score":90,"ts":"2025-01-22 12:00:15"}
{"name":"n1","score":93,"ts":"2025-01-22 12:00:20"}
{"name":"n1","score":93,"ts":"2025-01-22 12:00:25"}
{"name":"n1","score":95,"ts":"2025-01-22 12:00:30"}
{"name":"n1","score":99,"ts":"2025-01-22 12:00:55"}
{"name":"n2","score":99,"ts":"2025-01-22 12:01:20"}
```

Output:

```text
+I[n1, 94, 2025-01-22T12:01] # 90+93+95+99=377
```