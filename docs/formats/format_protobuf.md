# ProtoBuf Format

ProtoBuf is a binary format for serializing structured data. It is compact, efficient, and widely used.

Flink supports reading and writing data in the [ProtoBuf format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/protobuf/).

Astatine recommends using `protobuf_v2`. This version uses partial parsing to skip fields that are not used in `RowData`, which improves performance.

Astatine supports managing proto files and generating Java classes in the [astatine-format-protobuf](../../astatine-formats/astatine-format-protobuf) module.

This document explains how to use the ProtoBuf format in Astatine.


1. Create a proto file, such as `test.proto`, in the [astatine-format-protobuf proto directory](../../astatine-formats/astatine-format-protobuf/proto).

2. Define a message in the proto file.

   ```proto
   syntax = "proto3";
   package name.zicat.astatine.formats.protobuf;
   option java_package = "name.zicat.astatine.formats.protobuf";

   message NameScoreTs {
       string   name              = 1;
       int32    score             = 2;
       int64    ts                = 3;
   }
    ```
   
3. Rebuild the project.

   ```shell
   $ mvn clean install -Pdocker
   ......
   [INFO] ------------------------------------------------------------------------
   [INFO] BUILD SUCCESS
   [INFO] ------------------------------------------------------------------------
   [INFO] Total time:  3.745 s
   [INFO] Finished at: 2025-01-22T16:34:31+08:00
   ```

4. Use the proto message in SQL.

   ```sql
   CREATE TABLE target (
      name            STRING,
      score           INT,
      ts              BIGINT
   ) <@template.table_kafka_sink
       topic = 'test_topic_output'
       format = 'protobuf_v2'
       protobuf_v2\.message\-class\-name = 'name.zicat.astatine.formats.protobuf.Test$NameScoreTs' />
   ```
   
   Note: 
   - The `format` is `protobuf_v2`.
   - The `protobuf_v2.message-class-name` option is the fully qualified Java class name of the message generated from the proto file.
   - All options supported by the ProtoBuf format are documented in [Flink ProtoBuf format](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/protobuf/).
