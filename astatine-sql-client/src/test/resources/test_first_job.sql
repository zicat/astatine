<#import "env_local.ftl" as template>
<@template.udf_time />

CREATE TABLE source (
   name            STRING,
   vid             BIGINT,
   ts              BIGINT,
   numerator       BIGINT,
   denominator     BIGINT,
   productType     STRING,
   wk AS to_timestamp3(ts)
) <@template.table_kafka_source_property
    topic = 'insight_metric_vid'
    properties\.bootstrap\.servers = '10.11.2.10:9092,10.11.2.11:9092,10.11.2.12:9092' />

PRINT FROM source;