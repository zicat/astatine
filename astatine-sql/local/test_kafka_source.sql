<#import "env_local.ftl" as template>
<@template.udf_time />

CREATE TABLE source (
   name            STRING,
   score           INT,
   ts              BIGINT
   wk AS to_timestamp3(ts)
) <@template.table_kafka_source topic = 'test_topic'/>

PRINT FROM source;