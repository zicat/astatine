<#import "env_local.ftl" as template>
<@template.udf_statistics />
<@template.udf_math />
<@template.udf_time />

-- {"name":"n1","score":99,"ts":1736780400000}
-- {"name":"n1","score":99,"ts":1736780400000}
-- {"name":"n1","score":90,"ts":1736780400000}
-- {"name":"n1","score":91,"ts":1736780400000}
-- {"name":"n1","score":100,"ts":1740000000000}

CREATE TABLE source (
   name            STRING,
   score           INT,
   ts              BIGINT,
   event_time AS to_timestamp(ts),
   watermark FOR event_time AS event_time
) <@template.table_socket_source hostname = 'localhost' />

CREATE VIEW group_view AS
SELECT name
      ,percentile(score_temporary, 0.5) AS score_p50
      ,percentile(score_temporary, 0.9) AS score_p90
      ,hyper_log_log(score_differ_cnt_temporary) AS score_differ_cnt
FROM (
    SELECT name,TUMBLE_ROWTIME(event_time, INTERVAL '1' MINUTE) AS event_time
          ,percentile_temporary(score) AS score_temporary
          ,hyper_log_log_temporary(score) AS score_differ_cnt_temporary
    FROM source
    GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), name, modular(hash_code(name), 10)
) GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE), name;

PRINT FROM group_view;
