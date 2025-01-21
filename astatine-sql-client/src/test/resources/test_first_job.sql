CREATE TABLE source (
   name  STRING,
   score INT
) <@template.table_socket_source hostname = 'localhost' />

CREATE TABLE sink_elasticsearch (
    name            STRING,
    score           INT,
    PRIMARY KEY (name) NOT ENFORCED
) <@template.table_astatine_elasticsearch6_sink
    routing = '{name}'
    function\.id = 'default'
    hosts = 'http://localhost:9200'
    document\-type = 'doc'
    index = 'test_index' />

INSERT INTO sink_elasticsearch
SELECT name,score
FROM source;