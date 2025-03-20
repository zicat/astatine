# Streaming Eventtime Temporal Join Operator

The streaming eventtime temporal join operator is an eventtime-based streaming keyed operator that support to join two streams by eventtime like [Sql Eventtime Temporal Join](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/joins/#temporal-joins).

## Diff with Flink Sql Temporal Join:

1. Performance

    For the Sql Eventtime Temporal Join, it will create the deduplication operator before the temporal join operator for the right stream. For Stream Eventtime Temporal Join Operator this operator is removed.

2. Reliability 

    The streaming Eventtime Temporal Join Operator is expired state based on the eventtime ttl not processing time ttl that means more reliable in failure scenarios.

## Demo
```
<@template.setting_table exec\.state\.ttl = '1 min' />

CREATE TABLE source (
  name STRING,
  peer STRING,
  ts   TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
) <@template.table_socket_source hostname = 'host.docker.internal' port = '9990' />

CREATE TABLE source2 (
  name STRING,
  score INT,
  ts   TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
) <@template.table_socket_source hostname = 'host.docker.internal' port = '9991' />

-- create a stream from source2 with key by name
CREATE STREAM stream_keyed_by_name_source2
FROM source2 WITH (
  'product.type' = 'RowData'
) KEY BY WITH (
   'identity' = 'key_by_rowdata',
   'fields' = 'name'
);

-- create view with watermark from source with key by name join with stream_keyed_by_name_source2
CREATE VIEW temp_join_result WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
  'product.type' = 'RowData'
) KEY BY WITH (
   'identity' = 'key_by_rowdata',
   'fields' = 'name'
) CONNECT stream_keyed_by_name_source2 WITH (
  'identity' = 'temporal_join',
  'left.eventtime' = 'ts',
  'right.eventtime' = 'ts',
  'left.output.fields' = '*',
  'right.output.fields' = 'score AS right_source',
  'table.exec.state.ttl' = '10min',
  'join.type' = 'LEFT',
  'right.order.type' = 'LAST'
);

PRINT FROM temp_join_result;
```

Input 1:

```texg
$ nc -l 9990
{"name":"n1","peer":"p1","ts":"2024-09-09 16:00:02"}
{"name":"n1","peer":"p2","ts":"2024-09-09 16:00:03"}
{"name":"n1","peer":"p1","ts":"2024-09-09 16:00:04"}
{"name":"n1","peer":"p2","ts":"2024-09-09 16:00:05"}
{"name":"n100","peer":"p1","ts":"2024-09-09 16:10:02"}
```

Input 2:

```texg
$ nc -l 9991
{"name":"n1","ts":"2024-09-09 16:00:02","score":1}
{"name":"n100","ts":"2024-09-09 16:10:00","score":100}
```

Output:

```text
+I[n1, p1, 2024-09-09T16:00:02, 1]
+I[n1, p1, 2024-09-09T16:00:04, 1]
+I[n1, p2, 2024-09-09T16:00:03, 1]
+I[n1, p2, 2024-09-09T16:00:05, 1]
```

Note:
1. The identity of the operator is `temporal_join`.
2. The `left.eventtime` set the left stream eventtime field name.
3. The `right.eventtime` set the right stream eventtime field name.
4. The `left.output.fields` set the left stream output fields, split by `,`, the `*` means output all left input fields, using keyword `AS` to rename fieldName if necessary.
5. The `right.output.fields` set the right stream output fields, split by `,`, the `*` means output all left input fields, using keyword `AS` to rename fieldName if necessary.
6. The `join.type` set the join type including `LEFT`, `INNER`.
7. The `right.order.type` set using the `FIRST` OR `LAST` value of the right stream event to join.
8. The `table.exec.state.ttl` set the eventtime expired state ttl, the default value is 2 minutes.