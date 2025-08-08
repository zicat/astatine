# Session Tumble 2 Tumble Window Operator

The session tumble window operator is an eventtime-based rowdata-type streaming keyed operator that supports session-tumble windowing on the specified field.

```sql
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
   'tumble.interval' = '1min'
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
```

Input:

```texg
$ nc -l 9999
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:00:10","v1":111,"v2":"s1","v3":1101,"v4":1.1,"v5":1.001,"v6": true,"v7":1,"v8":11}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:01:09","v1":112,"v2":"s2","v3":1102,"v4":1.2,"v5":1.002,"v6":false,"v7":2,"v8":12}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:01:10","v1":113,"v2":"s3","v3":1103,"v4":1.3,"v5":1.003,"v6": true,"v7":3,"v8":13}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:02:09","v1":114,"v2":"s4","v3":1104,"v4":1.4,"v5":1.004,"v6":false,"v7":4,"v8":14}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:02:10","v1":115,"v2":"s5","v3":1105,"v4":1.5,"v5":1.005,"v6": true,"v7":5,"v8":15}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:02:11","v1":116,"v2":"s6","v3":1106,"v4":1.6,"v5":1.006,"v6":false,"v7":6,"v8":16}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:02:11","v1":117,"v2":"s7","v3":1107,"v4":1.7,"v5":1.007,"v6": true,"v7":7,"v8":17}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:03:10","v1":118,"v2":"s8","v3":1108,"v4":1.8,"v5":1.008,"v6":false,"v7":8,"v8":18}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:10:13","v1":119,"v2":"s9","v3":1109,"v4":1.9,"v5":1.009,"v6": true,"v7":9,"v8":19}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:11:13","v1":111,"v2":"s1","v3":1101,"v4":1.1,"v5":1.001,"v6":false,"v7":1,"v8":11}
{"id":"s2","f1":"fv1","ts":"2025-03-10 13:13:13","v1":112,"v2":"s2","v3":1102,"v4":1.2,"v5":1.002,"v6": true,"v7":2,"v8":12}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:13:15","v1":113,"v2":"s3","v3":1103,"v4":1.3,"v5":1.003,"v6":false,"v7":3,"v8":13}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:14:13","v1":114,"v2":"s4","v3":1104,"v4":1.4,"v5":1.004,"v6": true,"v7":4,"v8":14}
{"id":"s1","f1":"fv1","ts":"2025-03-10 13:14:15","v1":115,"v2":"s5","v3":1105,"v4":1.5,"v5":1.005,"v6":false,"v7":5,"v8":15}
```

Output:

```text
+I[s1, fv1, 2025-03-10T13:00:19.999, [111], [s1], [1101], [1.1], [1.001], [true], [1], [11], [1741611610000]]
+I[s1, fv1, 2025-03-10T13:01:19.999, [112, 113], [s2, s3], [1102, 1103], [1.2, 1.3], [1.002, 1.003], [false, true], [2, 3], [12, 13], [1741611669000, 1741611670000]]
+I[s1, fv1, 2025-03-10T13:02:19.999, [114, 115, 116, 117], [s4, s5, s6, s7], [1104, 1105, 1106, 1107], [1.4, 1.5, 1.6, 1.7], [1.004, 1.005, 1.006, 1.007], [false, true, false, true], [4, 5, 6, 7], [14, 15, 16, 17], [1741611729000, 1741611730000, 1741611731000, 1741611731000]]
+I[s1, fv1, 2025-03-10T13:03:19.999, [118], [s8], [1108], [1.8], [1.008], [false], [8], [18], [1741611790000]]
+I[s1, fv1, 2025-03-10T13:10:19.999, [119], [s9], [1109], [1.9], [1.009], [true], [9], [19], [1741612213000]]
+I[s1, fv1, 2025-03-10T13:11:19.999, [111], [s1], [1101], [1.1], [1.001], [false], [1], [11], [1741612273000]]
```

Note:
1. The identity of the operator is `session_tumble_2_tumble_window`.
2. The param `parallelism` is the parallelism of the operator, it must be a positive integer, default -1 means following previous stream parallelism.
3. The `fields` is the fields from `fields` in session_tumble_window.
4. The `eventtime` is the field name that points to the event time.
5. The `values` is the fields from `values` in session_tumble_window.
6. The `time-series.name` is the fields from `time-series.name` in session_tumble_window.
7. The `session.duration` is the fields from `session.duration` in session_tumble_window.
8. The `values.origin-type` is the input type of the `values` in session_tumble_window.
   
    The `session_tumble_2_tumble_window` will iterate the values records in binary type outputted by `session_tumble_window` operator,
    so you must specify the origin type of the values. 
    
9. The `tumble.interval` is the interval of the tumble window, it must be a valid time unit like `1min`, `1h`, etc.