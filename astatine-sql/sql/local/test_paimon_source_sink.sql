-- /usr/flink run-application -p 4 -Dtaskmanager.memory.managed.size=1m
<#import "env_local.ftl" as template>
<@template.setting_checkpointing />
<@template.udf_time />

CREATE CATALOG paimon WITH (
    'type'='paimon',
    'warehouse'='${template.paimon\.warehouse}'
);
USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.test_paimon (
     uid                BIGINT,
     ts                 BIGINT,
     `date`             INT,
     `hour`             INT
) PARTITIONED BY (`date`,`hour`)
WITH (
    'bucket' = '-1',
    'target-file-size' = '256 MB',
    'file.format' = 'parquet',
    'file.compression' = 'zstd',
    'file.compression.zstd-level' = '3',
    'order_strategy' = 'order',
    'snapshot.time-retained' = '20m',
    'snapshot.num-retained.min' = '10',
    'snapshot.num-retained.max' = '15',
    'snapshot.expire.execution-mode' = 'async',
    'partition.expiration-time' = '30d',
    'partition.timestamp-pattern' = '$date',
    'partition.timestamp-formatter' = 'yyyyMMdd',
    'order_by' = 'uid'
);

USE CATALOG default_catalog;

CREATE TABLE source (
    uid             BIGINT,
    ts              BIGINT
) <@template.table_socket_source hostname = 'localhost' />

INSERT INTO paimon.ods.test_paimon
SELECT uid,ts
      ,timestamp_to_date(ts, 'GMT') AS `date`
      ,timestamp_to_hour(ts, 'GMT') AS `hour`
FROM source;

PRINT FROM paimon.ods.test_paimon;