<#import "env_local.ftl" as template>
<@template.udf_basic />

CREATE TABLE source (
    content    STRING
) <@template.table_socket_source hostname = 'localhost' />

CREATE TABLE wechat_sink (
   headers 		Map<String, String>  METADATA,
   url   		STRING  			 METADATA,
   body         BYTES                METADATA
) <@template.table_http_property
    request_type = 'POST'
    read_timeout = '30s'
    connect_timeout = '30s' />

INSERT INTO wechat_sink
SELECT MAP['Content-Type', 'application/json']
      ,'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=e64089b8-51a2-48e4-a8bf-c9aeb0f23367'
      ,CAST(
        JSON_OBJECT(
            'msgtype' VALUE 'markdown'
           ,'markdown' VALUE JSON_OBJECT('content' VALUE content)
      ) AS BYTES)
FROM source;