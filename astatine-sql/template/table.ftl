<#macro table_kafka_source_property
        topic
        properties\.bootstrap\.servers = '${kafka\\.properties\\.bootstrap\\.servers}'
        scan\.startup\.mode ='latest-offset'
        format = 'json'
        dynamic_key_value...>
WITH (
    'connector' = 'kafka',
<#list dynamic_key_value?keys as p>
    '${p}' = '${dynamic_key_value[p]}',
</#list>
    'topic' = '${topic}',
    'properties.bootstrap.servers' = '${properties\.bootstrap\.servers}',
    'format' = '${format}',
    'scan.startup.mode' = '${scan\.startup\.mode}'
);
</#macro>