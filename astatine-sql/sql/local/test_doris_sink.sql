<@template.udf_time />

--{"name":"n1","score":10,"ts":1750217520000}

CREATE TABLE source (
  name              STRING,
  score             INT,
  ts                BIGINT
) <@template.table_socket_source hostname = 'localhost' />

CREATE TABLE sink_doris(
  name              STRING,
  score             INT,
  ts                BIGINT,
  `date`            DATE
) <@template.table_doris_sink_property
    fenodes = 'local:9030'
    table\.identifier = 'demo.name_score_test'
    username = 'root'
    password = '******'
    connection\.timeout = '15s'
    socket\.timeout = '5s'
    sink\.threads = 1
    retry\.times = '3'
    retry\.interval = '1s'
    sink\.parallelism = '1'
    sink\.batch\.bytes = '8388608'
    sink\.flush\-interval = '1s'
    sink\.group\-commit = 'async_mode'
    sink\.column\-separator = '\t'
    sink\.line\-delimiter = '\n'
    header\.properties\.load_to_single_tablet = 'true'
    auto\-create\-table = 'true'
    auto\-create\-table\.engine= 'DUPLICATE KEY(ts)'
    auto\-create\-table\.partition = 'PARTITION BY RANGE(`date`) ()'
    auto\-create\-table\.bucket = 'DISTRIBUTED BY RANDOM BUCKETS AUTO'
    auto\-create\-table\.properties\.compression = 'zstd'
    auto\-create\-table\.properties\.replication_allocation = 'tag.location.default: 2'
    auto\-create\-table\.properties\.dynamic_partition\.enable = 'true'
    auto\-create\-table\.properties\.dynamic_partition\.time_unit = 'DAY'
    auto\-create\-table\.properties\.dynamic_partition\.start = '-29'
    auto\-create\-table\.properties\.dynamic_partition\.end = '2'
    auto\-create\-table\.properties\.dynamic_partition\.prefix = 'p'
    auto\-create\-table\.properties\.dynamic_partition\.time_zone = 'GMT'
    auto\-create\-table\.properties\.dynamic_partition\.create_history_partition = 'true'
    auto\-create\-table\.properties\.group_commit_interval_ms = '3000'
    auto\-create\-table\.properties\.group_commit_data_bytes = '655360000' />

INSERT INTO sink_doris
SELECT name, score, ts, to_date(ts, 'GMT')
FROM source;

CREATE TABLE sink_doris_2(
  name              STRING,
  score             INT,
  ts                BIGINT,
  `date`            DATE
) <@template.table_doris_sink_property
    fenodes = 'localhost:9030'
    table\.identifier = 'demo.name_score_test_2'
    username = 'root'
    password = '******'
    connection\.timeout = '15s'
    socket\.timeout = '5s'
    sink\.threads = 1
    retry\.times = '3'
    retry\.interval = '1s'
    sink\.parallelism = '1'
    sink\.batch\.bytes = '8388608'
    sink\.flush\-interval = '1s'
    sink\.group\-commit = 'async_mode'
    sink\.column\-separator = '\t'
    sink\.line\-delimiter = '\n'
    auto\-create\-table = 'true'
    auto\-create\-table\.engine= 'UNIQUE KEY(name)'
    auto\-create\-table\.bucket = 'DISTRIBUTED BY HASH(name) BUCKETS 2'
    auto\-create\-table\.properties\.compression = 'zstd'
    auto\-create\-table\.properties\.replication_allocation = 'tag.location.default: 2'
    auto\-create\-table\.properties\.group_commit_interval_ms = '3000'
    auto\-create\-table\.properties\.group_commit_data_bytes = '655360000'/>

INSERT INTO sink_doris_2
SELECT name, score, ts, to_date(ts, 'GMT')
FROM source;

CREATE TABLE sink_doris_3(
  name              STRING,
  score             INT,
  ts                BIGINT,
  `date`            DATE
) <@template.table_doris_sink_property
    fenodes = 'localhost:9030'
    table\.identifier = 'demo.name_score_test_3'
    username = 'root'
    password = '******'
    connection\.timeout = '15s'
    socket\.timeout = '5s'
    sink\.threads = 1
    retry\.times = '3'
    retry\.interval = '1s'
    sink\.parallelism = '1'
    sink\.batch\.bytes = '8388608'
    sink\.flush\-interval = '1s'
    sink\.group\-commit = 'async_mode'
    sink\.column\-separator = '\t'
    sink\.line\-delimiter = '\n'
    auto\-create\-table = 'true'
    auto\-create\-table\.engine = 'AGGREGATE KEY(date,name,ts)'
    auto\-create\-table\.engine\.aggregate\-function\.score= 'REPLACE DEFAULT "0"'
    auto\-create\-table\.partition = 'PARTITION BY RANGE(`date`) ()'
    auto\-create\-table\.bucket = 'DISTRIBUTED BY HASH(name) BUCKETS 2'
    auto\-create\-table\.properties\.compression = 'zstd'
    auto\-create\-table\.properties\.replication_allocation = 'tag.location.default: 2'
    auto\-create\-table\.properties\.dynamic_partition\.enable = 'true'
    auto\-create\-table\.properties\.dynamic_partition\.time_unit = 'DAY'
    auto\-create\-table\.properties\.dynamic_partition\.start = '-29'
    auto\-create\-table\.properties\.dynamic_partition\.end = '2'
    auto\-create\-table\.properties\.dynamic_partition\.prefix = 'p'
    auto\-create\-table\.properties\.dynamic_partition\.time_zone = 'GMT'
    auto\-create\-table\.properties\.dynamic_partition\.create_history_partition = 'true'
    auto\-create\-table\.properties\.group_commit_interval_ms = '3000'
    auto\-create\-table\.properties\.group_commit_data_bytes = '655360000'/>

INSERT INTO  sink_doris_3
SELECT name, score, ts, to_date(ts, 'GMT')
FROM source;