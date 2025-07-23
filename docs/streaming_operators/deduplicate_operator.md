# Deduplicate Operator

The deduplicate operator is an eventtime-based streaming keyed operator that support to get the first/latest record of the key and discard the duplicate record.

```sql
<@template.setting_table exec\.state\.ttl = '1 min' />
<@template.setting_checkpointing aligned\-checkpoint\-timeout = '1 min'  interval = '1 min' timeout = '1 min' />

-- define streaming source
CREATE TABLE source (
  name      STRING ,
  score     INT,
  ts        TIMESTAMP(3),
  WATERMARK FOR ts AS ts
) <@template.table_socket_source hostname = 'host.docker.internal' />

CREATE VIEW stream_deduplicate_result WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
  'product.type' = 'RowData'
) KEY BY WITH (
   'identity' = 'key_by_rowdata',
   'fields' = 'name'
) PROCESS WITH (
    'identity' = 'deduplicate',
    'parallelism' = '2',
    'eventtime' = 'ts',
    'order.type' = 'ASC'
);

PRINT FROM stream_deduplicate_result;
```

Input:

```shell
$ nc -l 9999
{"name":"n1","score":1,"ts":"2024-12-03 12:00:00"}
{"name":"n1","score":2,"ts":"2024-12-03 12:00:01"}
{"name":"n1","score":3,"ts":"2024-12-03 12:00:02"}
```

Output:

```text
+I[n1, 1, 2024-12-03T12:00]
```

Note:
1. The identity of the operator is `deduplicate`.
2. The source of this operator must from a keyed operator `KEY BY WITH()` like above.
3. The row type of the source must be `RowData`.
4. The param `parallelism` is the parallelism of the operator, it must be a positive integer, default -1 means following previous stream parallelism.
5. The param `eventtime` is the field name that you want to deduplicate by event time.
6. The param `order.type` is the order type of the event time, it can be `ASC` or `DESC`.
7. The default ttl of value stored in ValueState is 2 minutes, user can set ttl by params `table.exec.state.ttl` as below.

   ```sql
   PROCESS WITH (
       'identity' = 'deduplicate',
       'parallelism' = '2',
       'eventtime' = 'ts',
       'order.type' = 'ASC',
       'table.exec.state.ttl' = '10 min'
    );
    ```
