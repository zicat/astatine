<#macro udf_time>
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS to_timestamp3 AS 'name.zicat.astatine.functions.ToTimestamp3' LANGUAGE JAVA;
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS to_long_timestamp AS 'name.zicat.astatine.functions.ToLongTimestamp' LANGUAGE JAVA;
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS timestamp_to_date AS 'name.zicat.astatine.functions.Timestamp2Date' LANGUAGE JAVA;
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS timestamp_to_hour AS 'name.zicat.astatine.functions.Timestamp2Hour' LANGUAGE JAVA;
</#macro>

<#macro udf_statistics
    percentile\.compression = '100'
    percentile\.compression\.temporary = '100'
    hyperloglog\.number\.slot = '4096'
    hyperloglog\.number\.per\.slot = '8' >
SET 'jobparam.percentile.compression' = '${percentile\.compression}';
SET 'jobparam.percentile.compression.temporary' = '${percentile\.compression\.temporary}';
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS percentile_temporary AS 'name.zicat.astatine.functions.statistics.percentile.PercentileTemporary' LANGUAGE JAVA;
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS percentile AS 'name.zicat.astatine.functions.statistics.percentile.Percentile' LANGUAGE JAVA;

SET 'jobparam.hyperloglog.number.slot' = '${hyperloglog\.number\.slot}';
SET 'jobparam.hyperloglog.number.per.slot' = '${hyperloglog\.number\.per\.slot}';
CREATE TEMPORARY FUNCTION IF NOT EXISTS hyper_log_log_temporary AS 'name.zicat.astatine.functions.statistics.hyperloglog.HyperloglogTemporary' LANGUAGE JAVA;
CREATE TEMPORARY FUNCTION IF NOT EXISTS hyper_log_log AS 'name.zicat.astatine.functions.statistics.hyperloglog.Hyperloglog' LANGUAGE JAVA;
</#macro>

<#macro udf_math
    hashcode\.seed = '12331233'>
SET 'jobparam.hashcode.seed' = '${hashcode\.seed}';
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS hash_code AS 'name.zicat.astatine.functions.math.HashCode' LANGUAGE JAVA;
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS modular AS 'name.zicat.astatine.functions.math.Modular' LANGUAGE JAVA;
</#macro>