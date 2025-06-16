# Http Sink Connector

Astatine support to sink data to http server.

## How to Create Http Json Sink Table

```sql
-- define http sink table
CREATE TABLE http_sink_table (
   headers 		Map<String, String>  METADATA,
   url   		STRING  			 METADATA,
   body         BYTES                METADATA
) WITH (
    'connector' = 'http',
    'request.type' = 'POST',
    'proxy' = '',
    'connect.timeout' = '10s',
    'read.timeout' = '10s',
    'retry.interval' = '1s',
    'retry.count' = '2',
    'async.queue.size' = '1024',
    'async.threads' = '5',
    'sink.parallelism' = '1',
    'code.400.fail' = 'false'
);

-- define with template
CREATE TABLE http_sink_table (
    headers 	Map<String, String>  METADATA,
    url   		STRING  			 METADATA,
    body        BYTES                METADATA
) <@template.table_http_property
    request\.type = 'POST'
    proxy = ''
    connect\.timeout = '10s'
    read\.timeout = '10s'
    retry\.interval = '1s'
    retry\.count = '2'
    async\.queue_size = '1024'
    async\.threads = '5'
    sink\.parallelism = '1' 
    code\.400\.fail = 'false'/>
```

## Connector Options

| Option            | Type     | Default | Description                                                                                    |
|-------------------|----------|---------|------------------------------------------------------------------------------------------------|
| request\.type     | enum     |         | Specify the http request type, supported type includes `GET`,`POST`,`PUT`,`DELETE`             |
| proxy             | string   | ''      | Specify the http proxy like: localhost:3333, default ''                                        |
| connect\.timeout  | duration | 10s     | Specify the socket connection timeout, default 10s                                             |
| read\.timeout     | duration | 10s     | Specify the socket read server response data timeout, default 10s                              |
| retry\.interval   | duration | 1s      | Specify the retry interval after previous http request failed, default 1s                      |
| retry\.count      | int      | 1       | Specify the retry count if request throw IOException OR response status code >= 500, default 1 |
| async\.queue.size | int      | 1024    | Specify the async http request queue size, default 1024                                        |
| async\.threads    | int      | 5       | Specify the async http thread pool size, default 5                                             |
| sink\.parallelism | Integer  | null    | Specify the http sink parallelism, default is extends previous operator parallelism            |
| code\.400\.fail   | boolean  | false   | Specify whether to fail the job if response status code is 400+, default false                 |

## Example

- Using http connector to sink data to WeChat.

    ```sql
    <#import "env_local.ftl" as template>
    <@template.udf_basic />
    
    CREATE TABLE source (
        content    STRING
    ) <@template.table_socket_source hostname = 'localhost' />
    
    CREATE TABLE wechat_sink (
        headers      Map<String, String>  METADATA,
        url   		 STRING  			 METADATA,
        body         BYTES                METADATA
    ) <@template.table_http_property
        request\.type = 'POST'
        read\.timeout = '30s'
        connect\.timeout = '30s' />
    
    INSERT INTO wechat_sink
    SELECT MAP['Content-Type', 'application/json']
          ,'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
          ,CAST(
              JSON_OBJECT(
                   'msgtype' VALUE 'markdown'
                  ,'markdown' VALUE JSON_OBJECT('content' VALUE content)
          ) AS BYTES)
    FROM source;
    ```

  Flink send the http request like follows:

    ```shell
      curl -X 'POST' 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx' \
           -H 'Content-Type: application/json' \
           -d '{"msgtype": "markdown", "markdown": "testing" }'
    ```