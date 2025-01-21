CREATE TABLE source (
  name STRING ,
  score INT
) <@template.table_socket_source hostname = 'localhost' />

-- astatine sql
CREATE STREAM stream_source
FROM source
MAP WITH (
    'identity' = 'row_2_pojo',
    'mapping.class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore',
    'return.class' = 'name.zicat.astatine.streaming.sql.parser.test.function.NameScore'
);
PRINT FROM stream_source;