# Select Operator

The select operator is a streaming map operator that supports selecting fields from the input stream and generating a new stream.

```sql
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
```

Input:

```texg
$ nc -l 9999
{"name":"NsN"}
```

Output:

```text
+I[NSN, nsn]
```

Note:
1. The identity of the operator is `select`.
2. The `expression` parameter specifies the fields to be selected supporting expressions.