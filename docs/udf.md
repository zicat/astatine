# Astatine User Defined Functions(UDF)

Astatine support to
develop [Flink UDF](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/functions/overview/),
UDF Source Code is in the module of [Astatine-Functions](../astatine-functions).

Existing UDFs in Astatine-Functions is defined as template
in [function.ftl](../astatine-sql/template/function.ftl).

Before using those functions, please include the related template macro first. e.g. import
to_timestamp3 function as follows:

```
<#import "env_local.ftl" as template>
<@template.udf_time />
```

## Scalar Functions

| SQL Function                                                                                                       | template name | Description                                                  |
|--------------------------------------------------------------------------------------------------------------------|---------------|--------------------------------------------------------------|
| [to_timestamp3(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/ToTimestamp3.java)        | udf_time      | Return the timestamp by long linux timestamp(13) of param    |
| [to_long_timestamp(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/ToLongTimestamp.java) | udf_time      | Return the long linux timestamp(13) by timestamp(3) of param |
| [hash_code(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/math/HashCode.java)           | udf_math      | Return the long hashcode of param                            |
| [modular(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/math/Modular.java)              | udf_math      | Return the int modular of param                              |

## Aggregation Functions

| SQL Function                                                                                                                                         | template name  | Description                                                                     |
|------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|---------------------------------------------------------------------------------|
| [percentile_temporary(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/percentile/PercentileTemporary.java)      | udf_statistics | Return the percentile temporary result by metrics of param                      |
| [percentile(param1, param2)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/percentile/Percentile.java)                | udf_statistics | Return the percentile result by metrics of param1 and double quantile of param2 |
| [hyper_log_log_temporary(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/hyperloglog/HyperloglogTemporary.java) | udf_statistics | Return the count distinct temporary result by metrics of param                  |
| [hyper_log_log(param)](../astatine-functions/src/main/java/name/zicat/astatine/functions/statistics/hyperloglog/Hyperloglog.java)                    | udf_statistics | Return the count distinct result by metrics of param                            |
