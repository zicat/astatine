# Doris Sink Connector

Astatine support to sink data to Doris(2.0+).

```sql
<@template.udf_time />

CREATE TABLE source (
  name              STRING,
  score             INT,
  ts                BIGINT
) <@template.table_socket_source hostname = 'localhost' />

CREATE TABLE sink_doris(
  name              STRING,
  score             INT,
  json_value        STRING, 
  ts                BIGINT,
  `date`            DATE
) <@template.table_doris_sink_property
    fenodes = 'localhost:9030'
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
    auto\-create\-table = 'true'
    auto\-create\-table\.fields\.type\.json_value = 'JSON'
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

INSERT INTO  sink_doris
SELECT name, score
     ,JSON_OBJECT(
            'name' VALUE name
           ,'score' VALUE score
     ), ts, to_date(ts, 'GMT')
FROM source;
```

### Connector Options


| Option                                              | Type     | Default    | Description                                                                                                                                                                                                                           |
|-----------------------------------------------------|----------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| fenodes                                             | string   |            | Specify the doris fe http address, split by `,` if set multi addresses                                                                                                                                                                |
| table\.identifier                                   | string   | ''         | Specify the doris table name to sink                                                                                                                                                                                                  |
| username                                            | string   |            | Specify the doris user name                                                                                                                                                                                                           |
| password                                            | string   |            | Specify the doris user password                                                                                                                                                                                                       |
| connection\.timeout                                 | duration | 15s        | Specify the socket connection timeout, default 15s                                                                                                                                                                                    |
| socket\.timeout                                     | duration | 5s         | Specify the socket read timeout, default 5s                                                                                                                                                                                           |
| sink\.threads                                       | int      | 1          | Specify the number of threads to write data for per slot, default 1                                                                                                                                                                   |
| retry\.times                                        | int      | 3          | Specify the number of retry times when write failed, default 3                                                                                                                                                                        |
| retry\.interval                                     | duration | 1s         | Specify the interval between retries, default 1s                                                                                                                                                                                      |
| sink\.parallelism                                   | int      | 1          | Specify the parallelism of sink operator, default 0 means follow the parallelism of previous flink operator                                                                                                                           |
| sink\.batch\.bytes                                  | int      | 8388608    | Specify the max size of data to write in one batch, default 8MB                                                                                                                                                                       |
| sink\.flush\-interval                               | duration | 1s         | Specify the interval to flush data to doris, default 1s                                                                                                                                                                               |
| sink\.group\-commit                                 | string   | async_mode | Specify the group commit mode, can be `sync_mode`,`async_mode`,`off_mode`, default `async_mode`, [Doris Group Commit](https://doris.apache.org/zh-CN/docs/data-operate/import/group-commit-manual?_highlight=commit&_highlight=group) |
| sink\.column\-separator                             | string   | \t         | Specify the column separator, default `\t`                                                                                                                                                                                            |
| sink\.line\-delimiter                               | string   | \n         | Specify the line delimiter, default `\n`                                                                                                                                                                                              |
| header\.properties\.*                               | string   |            | Specify the heads support by stream load request like `load_to_single_tablet`                                                                                                                                                         |
| auto\-create\-table                                 | boolean  | false      | Specify whether create table if not exist in doris when start to run start job                                                                                                                                                        |
| auto\-create\-table\.engine                         | string   |            | Specify the table engine when auto-create-table is true                                                                                                                                                                               |
| auto\-create\-table\.partition                      | string   |            | Specify the partition when auto-create-table is true                                                                                                                                                                                  |
| auto\-create\-table\.bucket                         | string   |            | Specify the bucket when auto-create-table is true                                                                                                                                                                                     |
| auto\-create\-table\.properties\.*                  | string   |            | Specify the table properties when auto-create-table is true, like `compression`, `replication_allocation`, `dynamic_partition.enable`, `dynamic_partition.time_unit`, `dynamic_partition.start`, `dynamic_partition.end`, etc.        |
| auto\-create\-table\.engine\.aggregate\-function\.* | string   |            | Specify the aggregate function when auto-create-table is true and the auto\-create\-table\.engine is `AGGREGATE KEY(...)`                                                                                                             |
| auto\-create\-table\.fields\.type\.*                | string   |            | Specity doris table field type that not supportted by flink like `JSON`                                                                                                                                                               |
NOTE:
1. Some header properties are not supported to set include: `label`, `group_commit`, `columns`, `format`, `column_separator`, `line_delimiter`, `Expect`, `Authorization`.
2. The username must have the permission to create table if `auto-create-table` is true.