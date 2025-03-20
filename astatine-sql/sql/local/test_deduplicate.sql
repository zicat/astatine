CREATE TABLE source (
  name      STRING ,
  score     INT,
  ts        TIMESTAMP(3),
  WATERMARK FOR ts AS ts
) <@template.table_socket_source hostname = 'localhost' />

CREATE VIEW stream_deduplicate_result WITH (
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
  'product.type' = 'RowData'
) KEY BY WITH (
   'identity' = 'key_by_rowdata',
   'fields' = 'name'
) PROCESS WITH (
    'identity' = 'deduplicate',
    'eventtime' = 'ts',
    'order.type' = 'ASC',
    'table.exec.state.ttl' = '10 sec'
);

PRINT FROM stream_deduplicate_result;