# HBase Sink Connector

Astatine support to sink data to hbase(version 2.2.x) .

# How to Create HBase Sink Table
```
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
```
# Connector Options

The HBase Sink Connector is based on the Flink HBase Sink Connector, so all options can be found in [Flink HBase SQL Connector](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/hbase/)

# Others

Please config param taskmanager.memory.task.off-heap.size like -Dtaskmanager.memory.task.off-heap.size=256m to avoid OOM when using HBase Sink Connector.