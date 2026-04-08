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
    byte\-delimiter = '10'
    format = 'json' >
WITH (
    'connector' = 'socket',
    'hostname' = '${hostname}',
    'port' = '${port}',
    'byte-delimiter' = '${byte\-delimiter}',
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
    code\.ignore = 'false'>
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
    'code.ignore' = '${code\.ignore}'
);
</#macro>

<#macro table_print_sink>
WITH (
    'connector' = 'print'
);
</#macro>

<#macro table_astatine_elasticsearch6_sink
    index
    document\-type
    hosts = '${elasticsearch\\.hosts}'
    dynamic_key_value...>
WITH (
    'connector' = 'astatine-elasticsearch-6',
    <#list dynamic_key_value?keys as p>
    '${p}' = '${dynamic_key_value[p]}',
    </#list>
    'hosts' = '${hosts}',
    'document-type' = '${document\-type}',
    'index' = '${index}'
);
</#macro>

<#macro table_hbase2_sink
    table\-name
    zookeeper\.quorum = '${hbase\\.zookeeper\\.quorum}'
    zookeeper\.znode\.parent = ''
    dynamic_key_value...>
WITH (
    'connector' = 'hbase-2.2',
    <#list dynamic_key_value?keys as p>
    '${p}' = '${dynamic_key_value[p]}',
    </#list>
    'table-name' = '${table\-name}',
    <#if zookeeper\.znode\.parent != ''>
    'zookeeper.znode.parent' = '${zookeeper\.znode\.parent}',
    </#if>
    'zookeeper.quorum' = '${zookeeper\.quorum}'
);
</#macro>

<#macro table_doris_sink_property
        fenodes
        table\.identifier
        username
        password
        connection\.timeout = '15s'
        socket\.timeout = '5s'
        sink\.threads = '1'
        retry\.times = '3'
        retry\.interval = '1s'
        sink\.parallelism = '0'
        sink\.batch\.bytes = '8388608'
        sink\.flush\-interval = '1s'
        sink\.group\-commit = 'async_mode'
        sink\.column\-separator = '\t'
        sink\.line\-delimiter = '\n'
        auto\-create\-table = 'false'
        auto\-create\-table\.engine = ''
        auto\-create\-table\.partition = ''
        auto\-create\-table\.bucket = 'DISTRIBUTED BY RANDOM BUCKETS AUTO'
        dynamic_key_value...>
WITH(
    'connector' = 'doris',
    'fenodes' = '${fenodes}',
    'table.identifier' = '${table\.identifier}',
    'username' = '${username}',
    'password' = '${password}',
    'connection.timeout' = '${connection\.timeout}',
    'socket.timeout' = '${socket\.timeout}',
    'sink.threads' = '${sink\.threads}',
    'retry.times' = '${retry\.times}',
    'retry.interval' = '${retry\.interval}',
    'sink.parallelism' = '${sink\.parallelism}',
    'sink.batch.bytes' = '${sink\.batch\.bytes}',
    'sink.flush-interval' = '${sink\.flush\-interval}',
    'sink.group-commit' = '${sink\.group\-commit}',
    'sink.column-separator' = '${sink\.column\-separator}',
    'sink.line-delimiter' = '${sink\.line\-delimiter}',
    <#list dynamic_key_value?keys as p>
        <#if p?starts_with("auto-create-table.properties.")>
    '${p}' = '${dynamic_key_value[p]}',
        </#if>
        <#if p?starts_with("header.properties.")>
    '${p}' = '${dynamic_key_value[p]}',
        </#if>
        <#if p?starts_with("auto-create-table.engine.aggregate-function.")>
    '${p}' = '${dynamic_key_value[p]}',
        </#if>
        <#if p?starts_with("auto-create-table.fields.type.")>
    '${p}' = '${dynamic_key_value[p]}',
        </#if>
    </#list>
    'auto-create-table' = '${auto\-create\-table}',
    'auto-create-table.engine' = '${auto\-create\-table\.engine}',
    'auto-create-table.partition' = '${auto\-create\-table\.partition}',
    'auto-create-table.bucket' = '${auto\-create\-table\.bucket}'
);
</#macro>