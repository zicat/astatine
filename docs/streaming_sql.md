# Astatine Streaming SQL

Astatine Streaming SQL is a superset of Flink SQL fully compatible with Flink SQL supporting orchestrating Flink Streaming Jobs.

This page lists all the supported statements supported in Astatine Streaming SQL for now:

- [CREATE STREAM identity FROM identity2 operators](#CREATE-STREAM-FROM)
- [CREATE VIEW identity FROM identity2 operators](#CREATE-VIEW-FROM)
- [INSERT INTO identity FROM identity2 operators](#INSERT-INTO-identity-FROM)

## CREATE STREAM FROM

`CREATE STREAM identity FROM identity2` support to create a stream from other streams, tables or views.

``` sql
-- flink sql
CREATE TABLE source (
  name STRING ,
  score INT
) <@template.table_socket_source hostname = 'localhost' />

-- astatine sql
CREATE STREAM stream_source 
FROM source
MAP WITH (
    'identity' = 'row_2_pojo',
    'class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore'
);

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

## INSERT INTO identity FROM

`INSERT INTO identity FROM` support to insert a stream to sink table.

```sql

-- flink sql
CREATE TABLE source (
  name STRING ,
  score INT
) <@template.table_socket_source hostname = 'localhost' />

-- astatine sql
CREATE STREAM map_source
FROM source
MAP WITH (
    'identity' = 'row_2_pojo',
    'class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore'
);

-- flink sql
CREATE TABLE print_table(
  name STRING,
  score DOUBLE
) <@template.table_print_sink />

-- astatine sql
INSERT INTO print_table FROM map_source
MAP WITH (
    'identity' = 'score_double'
);
```
