# Field Watermark Operator

The field watermark operator is a streaming operator that supports to assign watermark with a RowData-Type stream.

```sql
CREATE TABLE source (
    name        STRING ,
    ts          TIMESTAMP(3)
) <@template.table_socket_source hostname = 'host.docker.internal' />

CREATE VIEW field_watermark_result WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
    'product.type' = 'RowData'
) WATERMARK WITH (
    'identity' = 'field_watermark',
    'parallelism' = '2',
    'field' = 'ts',
    'delay' = '3s',
    'emit.on-event' = 'false'
);

PRINT FROM field_watermark_result;
```

Note:

1. The identity of this operator is `field_watermark`.
2. The product type of previous stream must be `RowData`.
3. Option `field` is used to specify the field name from previous stream that you want to assign watermark.
4. The param `parallelism` is the parallelism of the operator, it must be a positive integer, default -1 means following previous stream parallelism.
5. Option `delay` is used to specify the delay time of watermark.
6. Option `emit.on-event` is used to specify whether to emit the watermark on event time, default `false`. If `true` the operator will emit the watermark on event time, otherwise it will emit the watermark periodically.