-- define streaming source
CREATE TABLE source (
    name STRING ,
    score INT
) WITH (
  'connector' = 'socket',
  'hostname' = 'localhost',
  'port' = '9999',
  'byte-delimiter' = '10',
  'format' = 'json'
);

-- create logic view
CREATE VIEW source_double AS SELECT name, score * 2 FROM source;

-- keyword 'PRINT FROM' is used to print result to terminal
PRINT FROM source_double;