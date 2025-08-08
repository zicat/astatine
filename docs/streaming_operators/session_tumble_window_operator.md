# Session Tumble Window Operator

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
+I[s1, fv1, [111, 112], [s1, s2], [1101, 1102], [1.1, 1.2], [1.001, 1.002], [true, false], [1, 2], [11, 12], [1741611610000, 1741611669000]]
+I[s1, fv1, [113, 114], [s3, s4], [1103, 1104], [1.3, 1.4], [1.003, 1.004], [true, false], [3, 4], [13, 14], [1741611670000, 1741611729000]]
+I[s1, fv1, [115, 116, 117], [s5, s6, s7], [1105, 1106, 1107], [1.5, 1.6, 1.7], [1.005, 1.006, 1.007], [true, false, true], [5, 6, 7], [15, 16, 17], [1741611730000, 1741611731000, 1741611731000]]
+I[s1, fv1, [118], [s8], [1108], [1.8], [1.008], [false], [8], [18], [1741611790000]]
+I[s1, fv1, [119], [s9], [1109], [1.9], [1.009], [true], [9], [19], [1741612213000]]
+I[s1, fv1, [111], [s1], [1101], [1.1], [1.001], [false], [1], [11], [1741612273000]]
+I[s1, fv1, [113, 114], [s3, s4], [1103, 1104], [1.3, 1.4], [1.003, 1.004], [false, true], [3, 4], [13, 14], [1741612395000, 1741612453000]]
```

Note:
1. The identity of the operator is `session_tumble_window`.
2. The param `parallelism` is the parallelism of the operator, it must be a positive integer, default -1 means following previous stream parallelism.
3. The `fields` is the field names that you want to return in the output, only using first value in one session window.
4. The `eventtime` is the field name that points to the event time.
5. The `values` is the field names that you want to collect in the session window, the return type is `BINARY`.
6. The `time-series.name` is the name of the time series field, the operator will collect the time series in the session window, the type of this field is `BINARY`.
7. The `session.duration` is the window size.

   The start of window is the eventtime of first value. The session is deleted if no records in session.
8. The support types of values and the functions to collect them are:
   - `INT`/`DATE`/`TIME_WITHOUT_TIME_ZONE`/`INTERVAL_YEAR_MONTH`/`INTERVAL_DAY_TIME` -> `session_int_collect`
   - `STRING`/`VARCHAR`/`CHAR` -> `session_string_collect`
   - `LONG` -> `session_long_collect`
   - `FLOAT` -> `session_float_collect`
   - `DOUBLE` -> `session_double_collect`
   - `BOOLEAN` -> `session_bool_collect`
   - `TINYINT` -> `session_byte_collect`
   - `SMALLINT` -> `session_short_collect`
   - `BINARY`/`BYTES`/`VARBINARY` -> `session_binary_collect`
   
   NOTE: The functions are defined in the template `udf_session_value`.