<#include "setting.ftl">
<#include "function.ftl">
<#include "table.ftl">

<#assign kafka\.properties\.bootstrap\.servers = 'localhost:9092'>
<#assign socket\.hostname = 'localhost'>
<#assign elasticsearch\.hosts = 'localhost:9200'>
<#assign hbase\.zookeeper\.quorum = 'localhost:2181'>
<#assign paimon\.warehouse = 'file:///tmp/paimon'>