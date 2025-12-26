<#import "env_local.ftl" as template>

CREATE TABLE source (
    content    STRING
) <@template.table_socket_source hostname = 'localhost' />

CREATE TABLE wechat_sink (
    headers      Map<String, String>  METADATA,
    url          STRING              METADATA,
    body         BYTES                METADATA
) <@template.table_http_sink
    request\.type = 'POST'
    read\.timeout = '30s'
    connect\.timeout = '30s'
    code\.ignore = 'true' />

INSERT INTO wechat_sink
SELECT MAP['Content-Type', 'application/json']
      ,'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx'
      ,CAST(
          JSON_OBJECT(
               'msgtype' VALUE 'markdown'
              ,'markdown' VALUE JSON_OBJECT('content' VALUE content)
      ) AS BYTES)
FROM source;