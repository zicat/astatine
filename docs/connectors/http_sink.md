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
    'response.process-mode' = 'default',
    'response.process-mode.default.ignore' = 'false'
);

-- define with template
CREATE TABLE http_sink_table (
    headers 	Map<String, String>  METADATA,
    url   		STRING  			 METADATA,
    body        BYTES                METADATA
) <@template.table_http_sink
    request\.type = 'POST'
    proxy = ''
    connect\.timeout = '10s'
    read\.timeout = '10s'
    retry\.interval = '1s'
    retry\.count = '2'
    async\.queue\.size = '1024'
    async\.threads = '5'
    sink\.parallelism = '1'
    response\.process\-mode = 'default'
    response\.process\-mode\.default\.ignore = 'false'/>
```

## Connector Options

| Option            | Type     | Default | Description                                                                                                                         |
|-------------------|----------|---------|-------------------------------------------------------------------------------------------------------------------------------------|
| request\.type     | enum     |         | Specify the http request type, supported type includes `GET`,`POST`,`PUT`,`DELETE`                                                  |
| proxy             | string   | null    | Optional HTTP proxy, for example `localhost:3333`. An empty value also disables the proxy. |
| connect\.timeout  | duration | 10s     | Specify the socket connection timeout, default 10s                                                                                  |
| read\.timeout     | duration | 10s     | HTTP request timeout, applied to the initial request and each retry. |
| retry\.interval   | duration | 1s      | Specify the retry interval after previous http request failed, default 1s                                                           |
| retry\.count      | int      | 1       | Additional attempts after a transport error or a status rejected by the selected mode. Values below zero are treated as zero. |
| async\.queue.size | int      | 1024    | Specify the async http request queue size, default 1024                                                                             |
| async\.threads    | int      | 5       | Specify the async http thread pool size, default 5                                                                                  |
| sink\.parallelism | Integer  | null    | Specify the http sink parallelism, default is extends previous operator parallelism                                                 |
| response.process-mode | string | default | `default` or `specific_code` (case-insensitive). |
| response.process-mode.default.ignore | boolean | false | In `default` mode, accept all status codes when true; otherwise reject codes >= 400. |
| response.process-mode.specific_code.success-codes | string | 200 | In `specific_code` mode, only these comma-separated HTTP status codes are accepted. |

The `table_http_sink` template defaults to `sink.parallelism = '1'`; without
the template, the connector leaves parallelism to the planner.

## Response Handling

- `default`: codes below 400 succeed. Codes >= 400 are retried unless
  `response.process-mode.default.ignore = 'true'`. When ignored, each response
  with a code >= 400 increments `http_sink_failed_num` in the `code` metric group;
  it does not trigger a retry.
- `specific_code`: only codes in `response.process-mode.specific_code.success-codes`
  succeed, including explicitly listed 4xx/5xx codes. All other codes trigger
  retries, including unlisted 2xx codes. The default-mode ignore flag has no effect.
  Whitespace and duplicates are allowed; empty entries, non-numeric values and
  codes outside 100-599 are rejected during sink creation.

For example, accept 200, 204 and an expected conflict response:

```sql
CREATE TABLE http_sink_table (
    headers MAP<STRING, STRING> METADATA,
    url STRING METADATA,
    body BYTES METADATA
) <@template.table_http_sink
    request\.type = 'POST'
    response\.process\-mode = 'specific_code'
    response\.process\-mode\.specific_code\.success\-codes = '200,204,409' />
```

Transport errors still trigger retries in both modes, even when HTTP status
errors are ignored. `retry.count = '1'` means at most two attempts in total.
Responses are evaluated after automatic redirects; the response body is discarded,
so application-level errors in a JSON body are not inspected.

Requests run asynchronously. After retries are exhausted, the error is recorded
and thrown by the next sink invocation. The current sink does not flush pending
requests at checkpoints or on close, so an error on the last request may go
unreported. HTTP delivery is not exactly-once; retries can duplicate side effects,
so the receiving service should support idempotency when required.

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
    ) <@template.table_http_sink
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
