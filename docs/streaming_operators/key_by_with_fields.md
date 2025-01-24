# Key By with Fields Operator

Astatine provides a key-by-with-fields operator that can be used to convert a rowdata stream as a keyed rowdata stream by fields.

```sql
CREATE TABLE source (
  name              STRING,
  score             INT
) <@template.table_socket_source hostname = 'localhost' />

CREATE STREAM stream_by_name
FROM source WITH(
    'product.type' = 'RowData'
) KEY BY WITH(
    'identity' = 'key_by_rowdata',
    'fields' = 'name'
);

PRINT FROM stream_by_name;
```

Note:

- The identity of this operator is `key_by_rowdata`.
- The product type of previous stream must be `RowData`.
- Option `fields` is used to specify the fields from previous stream. The fields are separated by `,`.