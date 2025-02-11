# Disorder Discard Operator

The disorder discard operator is an eventtime-based streaming operator that support to emit rows in order and discard disorder rows.

```sql
CREATE TABLE source (
  name              STRING,
  ts                TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '3' SECOND
) <@template.table_socket_source hostname = 'host.docker.internal' />

CREATE STREAM stream_order_result
FROM source WITH(
    'product.type' = 'RowData'
) KEY BY WITH(
    'identity' = 'key_by_rowdata',
    'fields' = 'name'
) PROCESS WITH(
    'identity' = 'disorder_discard',
    'eventtime' = 'ts'
);

PRINT FROM stream_order_result;
```

Input:

```shell
$ nc -l 9999
{"name":"n1","ts":"2024-12-03 12:00:00"}
{"name":"n1","ts":"2024-12-03 12:00:02"}
{"name":"n1","ts":"2024-12-03 12:00:04"}
{"name":"n1","ts":"2024-12-03 12:00:03"}
{"name":"n1","ts":"2024-12-03 12:00:06"}
{"name":"n1","ts":"2024-12-03 12:00:01"}
{"name":"n1","ts":"2024-12-03 12:00:11"}
{"name":"n1","ts":"2024-12-03 12:00:15"}
```

Output:

```text
3> +I[n1, 2024-12-03T12:00]
3> +I[n1, 2024-12-03T12:00:02]
3> +I[n1, 2024-12-03T12:00:03]
3> +I[n1, 2024-12-03T12:00:04]
3> +I[n1, 2024-12-03T12:00:06]
3> +I[n1, 2024-12-03T12:00:11]
```

Note:
1. The identity of the operator is `disorder_discard`.
2. The source of this operator must from a keyed operator.
3. The row type of the source must be `RowData`.
4. The `eventtime` is the field name that you want to order by eventtime.
5. The output type of this operator is same as the source type.