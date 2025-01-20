<#import "env_local.ftl" as template>
<@template.udf_time />

CREATE TABLE source (
   name            STRING,
   score           INT,
   ts              BIGINT,
   event_time AS to_timestamp3(ts)
) <@template.table_kafka_source topic = 'test_topic'/>

CREATE TABLE target (
   name            STRING,
   score           INT,
   ts              BIGINT
) <@template.table_kafka_sink
    topic = 'test_topic_output'
    format = 'protobuf'
    protobuf\.message\-class\-name = 'name.zicat.astatine.formats.protobuf.Test$NameScoreTs'
    protobuf\.ignore\-parse\-errors = 'true' />

INSERT INTO target
SELECT name, score* 2, to_long_timestamp(event_time)
FROM source;