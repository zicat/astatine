-- flink sql
CREATE TABLE source (
  name STRING ,
  score INT
) <@template.table_socket_source hostname = 'localhost' />

-- astatine sql
CREATE STREAM stream_source
FROM source
MAP WITH (
    'identity' = 'row_2_pojo',
    'class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore'
);

CREATE STREAM stream_source_double
FROM stream_source
MAP WITH (
    'identity' = 'score_double'
);

PRINT FROM stream_source_double;