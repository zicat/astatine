# Field Value Changed Emit Operator

The field value changed emit operator is an eventtime-based streaming keyed operator that support to watch a field value change and emit the changed result.

```sql
<@template.setting tf_idle_state_retention_time='1 min' cp_alignment_timeout = '1 min' cp_interval = '2 min' cp_timeout = '2 min'/>

-- define streaming source
CREATE TABLE source (
  name STRING ,
  score INT,
  wk   TIMESTAMP(3),
  WATERMARK FOR wk AS wk
) <@template.table_socket_source hostname = 'localhost' />

CREATE VIEW field_changed_emit_result WITH (
    'expression.watermark' = 'WATERMARK FOR wk AS SOURCE_WATERMARK()'
) FROM source WITH (
  'product.type' = 'RowData'
) KEY BY WITH ('fields' = 'name')
PROCESS WITH (
    'identity' = 'field_value_watch_changed_emitter',
    'watch.field' = 'score',
    'eventtime' = 'wk'
);

PRINT FROM field_changed_emit_result;
```

Input:

```texg
$ nc -l 9999
{"name":"n1","score":1,"wk":"2024-12-03 12:00:00"}
{"name":"n1","score":1,"wk":"2024-12-03 12:00:01"}
{"name":"n1","score":2,"wk":"2024-12-03 12:00:02"}
{"name":"n1","score":2,"wk":"2024-12-03 12:00:03"}
{"name":"n1","score":3,"wk":"2024-12-03 12:00:04"}
{"name":"n1","score":3,"wk":"2024-12-03 12:00:05"}
{"name":"n1","score":1,"wk":"2024-12-03 12:00:06"}
{"name":"n1","score":1,"wk":"2024-12-03 12:00:07"}
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
       'eventtime' = 'wk'
    );
    ```
