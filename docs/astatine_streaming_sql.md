# Astatine Streaming SQL

Astatine Streaming SQL is a superset of Flink SQL fully compatible with Flink SQL supporting orchestrating Flink Streaming Jobs.

This page lists all the supported statements supported in Astatine Streaming SQL for now:

- CREATE STREAM identity FROM identity2 operators
- CREATE VIEW identity FROM identity2 operators

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

If you want to create a stream from a table with type of RowData, you can use the following sql:

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

CREATE VIEW view_source
WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM stream_source;

PRINT FROM view_source;
```

Creating view support to set watermark expression with `expression.watermark` in `WITH` clause.

The `SOURCE_WATERMARK()` is a built-in function to get the watermark of the source stream.

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
) KEY BY WITH('fields' = 'name')
PROCESS WITH (
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