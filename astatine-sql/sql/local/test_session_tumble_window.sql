-- define streaming source
--{"sid":"s1","vid":1,"peer":100,"score":1,"ts":"2025-03-10 13:00:10"}
--{"sid":"s1","vid":1,"peer":100,"score":2,"ts":"2025-03-10 13:01:09"}
--{"sid":"s1","vid":1,"peer":100,"score":3,"ts":"2025-03-10 13:01:10"}
--{"sid":"s1","vid":1,"peer":100,"score":4,"ts":"2025-03-10 13:02:09"}
--{"sid":"s1","vid":1,"peer":100,"score":5,"ts":"2025-03-10 13:02:10"}
--{"sid":"s1","vid":1,"peer":100,"score":6,"ts":"2025-03-10 13:02:11"}
--{"sid":"s1","vid":1,"peer":100,"score":7,"ts":"2025-03-10 13:02:11"}
--{"sid":"s1","vid":1,"peer":100,"score":8,"ts":"2025-03-10 13:03:10"}
--{"sid":"s1","vid":1,"peer":100,"score":9,"ts":"2025-03-10 13:10:13"}
--{"sid":"s1","vid":1,"peer":100,"score":10,"ts":"2025-03-10 13:11:13"}
--{"sid":"s2","vid":1,"peer":100,"score":10,"ts":"2025-03-10 13:13:13"}
--{"sid":"s1","vid":1,"peer":100,"score":11,"ts":"2025-03-10 13:13:15"}
--{"sid":"s1","vid":1,"peer":100,"score":12,"ts":"2025-03-10 13:14:13"}
--{"sid":"s1","vid":1,"peer":100,"score":13,"ts":"2025-03-10 13:14:15"}

-- +I[s1, 1, 100, 2025-03-10T13:01:10, [1741611610000, 1741611669000], [1, 2]]
-- +I[s1, 1, 100, 2025-03-10T13:02:10, [1741611670000, 1741611729000], [3, 4]]
-- +I[s1, 1, 100, 2025-03-10T13:03:10, [1741611730000, 1741611731000, 1741611731000], [5, 6, 7]]
-- +I[s1, 1, 100, 2025-03-10T13:04:10, [1741611790000], [8]]
-- +I[s1, 1, 100, 2025-03-10T13:11:13, [1741612213000], [9]]
-- +I[s1, 1, 100, 2025-03-10T13:12:13, [1741612273000], [10]]
-- +I[s1, 1, 100, 2025-03-10T13:14:15, [1741612395000, 1741612453000], [11, 12]]

CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS session_value_collect AS 'name.zicat.astatine.functions.SessionValueCollect' LANGUAGE JAVA;

CREATE TABLE source (
    sid          STRING,
    vid          INT,
    peer         BIGINT,
    score        BIGINT,
    ts           TIMESTAMP(3),
    WATERMARK FOR ts AS ts
) <@template.table_socket_source hostname = 'localhost' />

CREATE VIEW view_session_result WITH(
    'expression.watermark' = 'WATERMARK FOR ts AS SOURCE_WATERMARK()'
) FROM source WITH (
    'product.type' = 'RowData'
) KEY BY WITH(
    'identity' = 'key_by_rowdata',
    'fields' = 'sid'
) PROCESS WITH(
    'identity' = 'session_tumble_window',
    'fields' = 'vid AS vendor_id, peer AS peer_id, sid',
    'eventtime' = 'ts',
    'values' = 'score AS score_1',
    'time-series.name' = 'time_series',
    'session.duration' = '1min'
);

CREATE VIEW aa AS
SELECT a.sid, a.vendor_id, a.peer_id, a.ts,t.collect_result AS time_collect, t2.collect_result AS score_collect
FROM view_session_result AS a
LEFT JOIN LATERAL TABLE(session_value_collect(a.time_series)) AS t ON TRUE
LEFT JOIN LATERAL TABLE(session_value_collect(a.score_1)) AS t2 ON TRUE
WHERE a.sid = 's1';

PRINT FROM aa;