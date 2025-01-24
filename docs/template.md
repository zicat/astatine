# Astatine Sql Template

Astatine Sql Template is a feature that allows you to use a template to generate a sql script. 

The benefit of using template is that you can reuse the sql logic mostly and configure difference like source sink url in env_xxx.ftl.

This section is introduced how to use it.

## Define Flink Configuration
```sql
<#import "env_local.ftl" as template>

-- set the flink job restart strategy policy
<@template.setting_restart_strategy_policy type = 'fixed-delay' fixed\-delay\.attempts = '2147483647' delay = '10 s' />

-- set table state ttl
<@template.setting_table exec\.state\.ttl = '4 min' />

-- set the flink job checkpointing
<@template.setting_checkpointing
    mode = 'EXACTLY_ONCE' aligned\-checkpoint\-timeout = '1 min' interval = '2 min' timeout = '2 min' unaligned\.enabled = 'false'
    max\-concurrent\-checkpoint = '2' min\-pause = '2 s' externalized\-checkpoint\-retention = 'RETAIN_ON_CANCELLATION' tolerable\-failed\-checkpoints = '5' />
    
-- set other flink params that not supported by template
SET 'table.exec.source.idle-timeout'= '2 min';    
```

Note: User can add other env by adding related template to [astatine-sql/template](../astatine-sql/template)

## [Define Function](udf_develop.md)


## Define Source Sink

- [Kafka Source And Sink](connectors/kafka_source_sink.md)
- [Http Sink](connectors/http_sink.md)
- [Socket Source](connectors/socket_source.md)