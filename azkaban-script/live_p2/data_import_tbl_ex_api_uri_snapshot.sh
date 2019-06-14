#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

####  live_ip_list  这个暂时没有上报数据
####
####


echo '############## 插入新的api uri信息 start ###############'
echo '################# 拆分URI数据 插入app uri hive表 start   ########################'

insert_sql="
INSERT INTO TABLE live_p2.tbl_ex_api_uri_snapshot
SELECT a.record_time,a.trace_id,a.app_package_name,'${ias_source}',
       a.uri,
       if (parse_url(a.uri,'HOST') IS NULL, a.uri, parse_url(a.uri,'HOST'))
FROM (
      SELECT record_time,trace_id,app_package_name,r1.uri
      FROM ias_p2.tbl_ex_live_ip_list_data_origin_orc as t
      LATERAL VIEW explode(uri_list) r1 AS uri
      WHERE dt='${day}' AND uri_list IS NOT NULL
) as a
"

executeHiveCommand "${insert_sql}"

echo '################# 拆分URI数据 插入app uri hive表 end   ########################'

echo '################# 插入app uri mysql表 start   ########################'

mysql_table="tbl_live_p2_api_uri"

hive_sql2="
SELECT biz_name,data_source,host_ip,COUNT(1) AS count,MAX(record_time) AS lastest_update_time
FROM live_p2.tbl_ex_api_uri_snapshot
GROUP BY biz_name,data_source,host_ip
"

execSqlOnMysql "DELETE FROM ${mysql_table}"

hiveSqlToMysqlNoDelete "${hive_sql2}" "${mysql_table}" "biz_name,data_source,host_ip,count,lastest_update_time"

echo '################# 插入app uri mysq表 end   ########################'

echo '############## 插入新的api uri信息 end ################'
