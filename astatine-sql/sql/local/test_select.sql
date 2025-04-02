CREATE TABLE source (
  name      STRING
) <@template.table_socket_source hostname = 'localhost' />

CREATE STREAM stream_select
FROM source WITH(
    'product.type' = 'RowData'
) MAP WITH(
    'identity' = 'select',
    'expression' = 'UPPER(name) AS n1, LOWER(name) AS n2'
);

PRINT FROM stream_select;