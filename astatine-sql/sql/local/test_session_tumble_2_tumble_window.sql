<@template.udf_session_value />

CREATE TABLE source (
    id           STRING,
    f1           STRING,
    v1           INT,
    v2           STRING,
    v3           BIGINT,
    v4           FLOAT,
    v5           DOUBLE,
    v6           BOOLEAN,
    v7           TINYINT,
    v8           SMALLINT,
    ts           TIMESTAMP(3),
    WATERMARK FOR ts AS ts
) <@template.table_socket_source hostname = 'localhost' />

CREATE VIEW view_session_result WITH(
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
    'product.type' = 'RowData'
) KEY BY WITH(
    'identity' = 'key_by_rowdata',
    'fields' = 'id'
) PROCESS WITH(
    'identity' = 'session_tumble_window',
    'parallelism' = '1',
    'fields' = 'id,f1',
    'eventtime' = 'ts',
    'values' = 'v1 AS vv1,v2 AS vv2,v3 AS vv3,v4 AS vv4,v5 AS vv5,v6 AS vv6,v7 AS vv7,v8 AS vv8',
    'time-series.name' = 'time_series',
    'session.duration' = '1min'
) KEY BY WITH(
    'identity' = 'key_by_rowdata',
    'fields' = 'id'
) PROCESS WITH(
   'identity' = 'session_tumble_2_tumble_window',
   'parallelism' = '1',
   'fields' = 'id,f1',
   'eventtime' = 'ts',
   'values' = 'vv1 AS vvv1,vv2 AS vvv2,vv3 AS vvv3,vv4 AS vvv4,vv5 AS vvv5,vv6 AS vvv6,vv7 AS vvv7,vv8 AS vvv8',
   'values.origin-type' = 'INT,STRING,BIGINT,FLOAT,DOUBLE,BOOLEAN,TINYINT,SMALLINT',
   'time-series.name' = 'time_series',
   'session.duration' = '1min',
   'tumble.interval' = '20s'
);

CREATE VIEW aa AS
SELECT a.id,a.f1,ts
      ,t1.collect_result AS vv1_list
      ,t2.collect_result AS vv2_list
      ,t3.collect_result AS vv3_list
      ,t4.collect_result AS vv4_list
      ,t5.collect_result AS vv5_list
      ,t6.collect_result AS vv6_list
      ,t7.collect_result AS vv7_list
      ,t8.collect_result AS vv8_list
      ,t9.collect_result AS time_series_list
FROM view_session_result AS a
LEFT JOIN LATERAL TABLE(session_int_collect(vvv1)) AS t1 ON TRUE
LEFT JOIN LATERAL TABLE(session_string_collect(vvv2)) AS t2 ON TRUE
LEFT JOIN LATERAL TABLE(session_long_collect(vvv3)) AS t3 ON TRUE
LEFT JOIN LATERAL TABLE(session_float_collect(vvv4)) AS t4 ON TRUE
LEFT JOIN LATERAL TABLE(session_double_collect(vvv5)) AS t5 ON TRUE
LEFT JOIN LATERAL TABLE(session_bool_collect(vvv6)) AS t6 ON TRUE
LEFT JOIN LATERAL TABLE(session_byte_collect(vvv7)) AS t7 ON TRUE
LEFT JOIN LATERAL TABLE(session_short_collect(vvv8)) AS t8 ON TRUE
LEFT JOIN LATERAL TABLE(session_long_collect(time_series)) AS t9 ON TRUE
WHERE a.id = 's1';

PRINT FROM aa;