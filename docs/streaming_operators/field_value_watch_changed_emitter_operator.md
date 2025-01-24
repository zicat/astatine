# Field Value Changed Emit Operator

The field value changed emit operator is an eventtime-based streaming keyed operator that support to watch a field value change and emit the changed result.

```sql
<@template.setting_table exec\.state\.ttl = '1 min' />
<@template.setting_checkpointing aligned\-checkpoint\-timeout = '1 min'  interval = '1 min' timeout = '1 min' />

-- define streaming source
CREATE TABLE source (
  name STRING ,
  score INT,
  ts   TIMESTAMP(3),
  WATERMARK FOR ts AS ts
) <@template.table_socket_source hostname = 'host.docker.internal' />

CREATE VIEW field_changed_emit_result WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
  'product.type' = 'RowData'
) KEY BY WITH (
   'identity' = 'key_by_rowdata',
   'fields' = 'name'
) PROCESS WITH (
    'identity' = 'field_value_watch_changed_emitter',
    'watch.field' = 'score',
    'eventtime' = 'ts'
);

PRINT FROM field_changed_emit_result;
```

Input:

```shell
$ nc -l 9999
{"name":"n1","score":1,"ts":"2024-12-03 12:00:00"}
{"name":"n1","score":1,"ts":"2024-12-03 12:00:01"}
{"name":"n1","score":2,"ts":"2024-12-03 12:00:02"}
{"name":"n1","score":2,"ts":"2024-12-03 12:00:03"}
{"name":"n1","score":3,"ts":"2024-12-03 12:00:04"}
{"name":"n1","score":3,"ts":"2024-12-03 12:00:05"}
{"name":"n1","score":1,"ts":"2024-12-03 12:00:06"}
{"name":"n1","score":1,"ts":"2024-12-03 12:00:07"}
```

Output:

```text
+I[n1, 1, 2024-12-03T12:00]
+I[n1, 2, 2024-12-03T12:00:02]
+I[n1, 3, 2024-12-03T12:00:04]
+I[n1, 1, 2024-12-03T12:00:06]
```

Note:
1. The identity of the operator is `field_value_watch_changed_emitter`.
2. The `watch.field` is the field name that you want to watch the value change.
3. The source of this operator must from a keyed operator `KEY BY WITH()` like above.
4. The row type of the source must be `RowData`.
5. The operator is based on eventtime trigger, if the record received is disorder, the operator will discard the record.
6. The default ttl of value stored in ValueState is 2 minutes, user can set ttl by params `table.exec.state.ttl` as below.

   ```sql
   PROCESS WITH (
       'identity' = 'field_value_watch_changed_emitter',
       'watch.field' = 'score',
       'table.exec.state.ttl' = '10 min',
       'eventtime' = 'ts'
    );
    ```
