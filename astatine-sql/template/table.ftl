<#macro table_kafka_source
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
    'scan.startup.mode' = '${scan\.startup\.mode}',
    'format' = '${format}'
);
</#macro>

<#macro table_kafka_sink
        topic
        format = 'json'
        properties\.bootstrap\.servers = '${kafka\\.properties\\.bootstrap\\.servers}'
        properties\.acks = '1'
        properties\.compression\.type = 'zstd'
        properties\.linger\.ms = '100'
        properties\.buffer\.memory = '262144000'
        properties\.batch\.size = '122880'
        sink\.partitioner = 'default'
        dynamic_key_value...>
WITH (
    'connector' = 'kafka',
    <#list dynamic_key_value?keys as p>
    '${p}' = '${dynamic_key_value[p]}',
    </#list>
    'topic' = '${topic}',
    'format' = '${format}',
    'properties.bootstrap.servers' = '${properties\.bootstrap\.servers}',
    'properties.acks' = '${properties\.acks}',
    'properties.compression.type' = '${properties\.compression\.type}',
    'properties.linger.ms' = '${properties\.linger\.ms}',
    'properties.buffer.memory' = '${properties\.buffer\.memory}',
    'properties.batch.size' = '${properties\.batch\.size}',
    'sink.partitioner' = '${sink\.partitioner}'
);
</#macro>

<#macro table_socket_source
    hostname = '${socket\\.hostname}'
    port = '9999'
    byte\.delimiter = '10'
    format = 'json' >
WITH (
    'connector' = 'socket',
    'hostname' = '${hostname}',
    'port' = '${port}',
    'byte-delimiter' = '${byte\.delimiter}',
    'format' = '${format}'
);
</#macro>

<#macro table_http_sink
    request\.type
    proxy = ''
    connect\.timeout = '10s'
    read\.timeout = '10s'
    retry\.interval = '1s'
    retry\.count = '1'
    async\.queue\.size = '1024'
    async\.threads = '5'
    sink\.parallelism = '1'
    code\.400\.fail = 'false'>
WITH (
    'connector' = 'http',
    'request.type' = '${request\.type}',
    'proxy' = '${proxy}',
    'connect.timeout' = '${connect\.timeout}',
    'read.timeout' = '${read\.timeout}',
    'retry.interval' = '${retry\.interval}',
    'retry.count' = '${retry\.count}',
    'async.queue.size' = '${async\.queue\.size}',
    'async.threads' = '${async\.threads}',
    'sink.parallelism' = '${sink\.parallelism}',
    'code.400.fail' = '${code\.400\.fail}'
);
</#macro>