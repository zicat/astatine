CREATE TABLE source (
  name              STRING,
  score             INT
) <@template.table_socket_source hostname = 'localhost' />


CREATE VIEW source_double AS SELECT name, score * 2 AS score FROM source;

PRINT FROM source_double;