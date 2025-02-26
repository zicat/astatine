# Astatine Built-In Functions

Astatine Built-In Functions in Astatine-Functions is defined as template
in [function.ftl](../astatine-sql/template/function.ftl).

Before using those functions, please include the related template macro first. e.g. import
to_timestamp function as follows:

```
<#import "env_local.ftl" as template>
<@template.udf_time />
```

## Scalar Functions

| SQL Function                                                                                                       | template name | Description                                                                                                                           |
|--------------------------------------------------------------------------------------------------------------------|---------------|---------------------------------------------------------------------------------------------------------------------------------------|
| [to_timestamp(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/ToTimestamp3.java)         | udf_time      | Return the timestamp by long linux timestamp(13) of param                                                                             |
| [to_long_timestamp(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/ToLongTimestamp.java) | udf_time      | Return the long linux timestamp(13) by timestamp(3) of param                                                                          |
| [to_date(param1,param2)](../astatine-functions/src/main/java/name/zicat/astatine/functions/Timestamp2Date.java)    | udf_time      | Return the `DATE` TYPE date by long or timestamp(3) of param1 and option timezone of params2 like 'GMT+8',default params2 is 'GMT'    |
| [to_hour(param1,param2)](../astatine-functions/src/main/java/name/zicat/astatine/functions/Timestamp2Hour.java)    | udf_time      | Return the `INT` TYPE hour(HH) by long or timestamp(3) of param1 and option timezone of params2 like 'GMT+8',default params2 is 'GMT' |
| [hash_code(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/math/HashCode.java)           | udf_math      | Return the long hashcode of param                                                                                                     |
| [modular(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/math/Modular.java)              | udf_math      | Return the int modular of param                                                                                                       |
| [line_separator()](../astatine-functions/src/main/java/name/zicat/astatine/functions/LineSeparator.java)           | udf_basic     | Return the line separator of system                                                                                                   |

## Aggregation Functions

| SQL Function                                                                                                                                         | template name  | Description                                                                     |
|------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|---------------------------------------------------------------------------------|
| [percentile_temporary(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/percentile/PercentileTemporary.java)      | udf_statistics | Return the percentile temporary result by metrics of param                      |
| [percentile(param1, param2)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/percentile/Percentile.java)                | udf_statistics | Return the percentile result by metrics of param1 and double quantile of param2 |
| [hyper_log_log_temporary(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/hyperloglog/HyperloglogTemporary.java) | udf_statistics | Return the count distinct temporary result by metrics of param                  |
| [hyper_log_log(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/hyperloglog/Hyperloglog.java)                    | udf_statistics | Return the count distinct result by metrics of param                            |
