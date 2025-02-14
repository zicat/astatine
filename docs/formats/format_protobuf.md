# ProtoBuf Format

The ProtoBuf Format is a binary format that is used to serialize structured data. It is a popular format for serializing data in a compact and efficient way.

Flink supports reading and writing data in the [ProtoBuf Format](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/formats/protobuf/).

Astatine support to manage the Proto File and build as java class in the module of [astatine-format-protobuf](../../astatine-formats/astatine-format-protobuf).

This document introduces how to use the ProtoBuf Format in Astatine.


1. Create a proto file like `test.proto` in the module of [astatine-format-protobuf](../../astatine-formats/astatine-format-protobuf/proto).  

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
   
3. Rebuild project

   ```shell
   $ mvn clean install -Pdocker
   ......
   [INFO] ------------------------------------------------------------------------
   [INFO] BUILD SUCCESS
   [INFO] ------------------------------------------------------------------------
   [INFO] Total time:  3.745 s
   [INFO] Finished at: 2025-01-22T16:34:31+08:00
   ```

4. Use proto file in the SQL

   ```sql
   CREATE TABLE target (
      name            STRING,
      score           INT,
      ts              BIGINT
   ) <@template.table_kafka_sink
       topic = 'test_topic_output'
       format = 'protobuf'
       protobuf\.message\-class\-name = 'name.zicat.astatine.formats.protobuf.Test$NameScoreTs' />
   ```
   
   Note: 
   - The `format` is `protobuf`.
   - The `protobuf.message-class-name` is the full class name of the message defined in the proto file.
   - All options supported by the ProtoBuf Format can be found in [Flink ProtoBuf Format](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/formats/protobuf/).