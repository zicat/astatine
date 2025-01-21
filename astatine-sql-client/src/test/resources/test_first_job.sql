CREATE TABLE source (
   rowkey  BYTES,
   column1 BYTES,
   column2 BYTES,
   column3 BYTES,
   column4 BYTES
) <@template.table_socket_source hostname = 'localhost' />

CREATE TABLE hTable (
     rowkey           BYTES,
     family1          ROW<column1 BYTES, column2 BYTES>,
     family2          ROW<column3 BYTES, column4 BYTES>,
     PRIMARY KEY (rowkey) NOT ENFORCED
) <@template.table_hbase2_sink
    table\-name = 'test_table' />

INSERT INTO hTable
SELECT rowkey
      ,ROW(column1, column2)
      ,ROW(column3, column4)
FROM source;