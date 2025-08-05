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
    'parallelism' = '2',
    'fields' = 'id,f1',
    'eventtime' = 'ts',
    'values' = 'v1 AS vv1,v2 AS vv2,v3 AS vv3,v4 AS vv4,v5 AS vv5,v6 AS vv6,v7 AS vv7,v8 AS vv8',
    'time-series.name' = 'time_series',
    'session.duration' = '1min'
);

CREATE VIEW aa AS
SELECT a.id,a.f1
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
LEFT JOIN LATERAL TABLE(session_int_collect(vv1)) AS t1 ON TRUE
LEFT JOIN LATERAL TABLE(session_string_collect(vv2)) AS t2 ON TRUE
LEFT JOIN LATERAL TABLE(session_long_collect(vv3)) AS t3 ON TRUE
LEFT JOIN LATERAL TABLE(session_float_collect(vv4)) AS t4 ON TRUE
LEFT JOIN LATERAL TABLE(session_double_collect(vv5)) AS t5 ON TRUE
LEFT JOIN LATERAL TABLE(session_bool_collect(vv6)) AS t6 ON TRUE
LEFT JOIN LATERAL TABLE(session_byte_collect(vv7)) AS t7 ON TRUE
LEFT JOIN LATERAL TABLE(session_short_collect(vv8)) AS t8 ON TRUE
LEFT JOIN LATERAL TABLE(session_long_collect(time_series)) AS t9 ON TRUE
WHERE a.id = 's1';

PRINT FROM aa;