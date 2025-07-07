CREATE TABLE source (
    name        STRING ,
    ts          TIMESTAMP(3)
) <@template.table_socket_source hostname = 'localhost' />

CREATE VIEW field_watermark_result WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
    'product.type' = 'RowData'
) WATERMARK WITH (
    'identity' = 'field_watermark',
    'field' = 'ts',
    'delay' = '3s',
    'emit.on-event' = 'false'
);

PRINT FROM field_watermark_result;