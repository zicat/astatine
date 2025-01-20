<#macro udf_time>
CREATE TEMPORARY SYSTEM FUNCTION IF NOT EXISTS to_timestamp3 AS 'name.zicat.astatine.functions.ToTimestamp3' LANGUAGE JAVA;
</#macro>