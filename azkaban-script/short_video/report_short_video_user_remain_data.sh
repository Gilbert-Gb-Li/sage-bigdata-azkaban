#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 抖音视频用户留存数据统计 start #####################"
date=$1

mysql_table="tbl_report_short_video_daily_data"
next_remain_date=`date -d "-1 day $date" +%Y-%m-%d`
third_remain_date=`date -d "-2 day $date" +%Y-%m-%d`
seven_remain_date=`date -d "-6 day $date" +%Y-%m-%d`

tmp_mysql_remain_table="tmp_tbl_report_short_video_user_remain_data"

create_tmp_mysql_remain_table_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_remain_table} (new_date CHAR(10),remain_count INT(11),remain_type TINYINT(2))"

execSqlOnMysql "${create_tmp_mysql_remain_table_sql}"


next_remain_sql="select '${next_remain_date}',count(distinct t2.author_id) as next_remain,1 from 
(select dt,author_id from ias.tbl_ex_short_video_data_origin_orc where dt='${next_remain_date}' and author_id!='' and author_id is not null and from_unixtime( cast( substr(video_create_time,0,10) as bigint),'yyyy-MM-dd')='${next_remain_date}') as t1 
join (
select dt,author_id from ias.tbl_ex_short_video_data_origin_orc where dt='${date}' and author_id!='' and author_id is not null and from_unixtime( cast( substr(video_create_time,0,10) as bigint),'yyyy-MM-dd')='${date}'
) as t2 on t1.author_id=t2.author_id where t2.author_id is not null and t2.author_id!=''"

hiveSqlToMysql "${next_remain_sql}" "${next_remain_date}" "${tmp_mysql_remain_table}" "new_date,remain_count,remain_type" "new_date"


third_remain_sql="select '${third_remain_date}',count(distinct t2.author_id) as next_remain,3 from 
(select dt,author_id from ias.tbl_ex_short_video_data_origin_orc where dt='${third_remain_date}' and author_id!='' and author_id is not null and from_unixtime( cast( substr(video_create_time,0,10) as bigint),'yyyy-MM-dd')='${third_remain_date}') as t1 
join (
select dt,author_id from ias.tbl_ex_short_video_data_origin_orc where dt='${date}' and author_id!='' and author_id is not null and from_unixtime( cast( substr(video_create_time,0,10) as bigint),'yyyy-MM-dd')='${date}'
) as t2 on t1.author_id=t2.author_id where t2.author_id is not null and t2.author_id!=''"

hiveSqlToMysql "${third_remain_sql}" "${third_remain_date}" "${tmp_mysql_remain_table}" "new_date,remain_count,remain_type" "new_date"

seven_remain_sql="select '${seven_remain_date}',count(distinct t2.author_id) as next_remain,7 from 
(select dt,author_id from ias.tbl_ex_short_video_data_origin_orc where dt='${seven_remain_date}' and author_id!='' and author_id is not null and from_unixtime( cast( substr(video_create_time,0,10) as bigint),'yyyy-MM-dd')='${seven_remain_date}') as t1 
join (
select dt,author_id from ias.tbl_ex_short_video_data_origin_orc where dt='${date}' and author_id!='' and author_id is not null and from_unixtime( cast( substr(video_create_time,0,10) as bigint),'yyyy-MM-dd')='${date}'
) as t2 on t1.author_id=t2.author_id where t2.author_id is not null and t2.author_id!=''"

hiveSqlToMysql "${seven_remain_sql}" "${seven_remain_date}" "${tmp_mysql_remain_table}" "new_date,remain_count,remain_type" "new_date·"


update_next_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT new_date,remain_count,remain_type
  FROM $tmp_mysql_remain_table
  WHERE new_date='${next_remain_date}' AND remain_type='1'
) b
ON a.stat_date=b.new_date
SET a.short_video_user_next_remain=b.remain_count
WHERE a.stat_date='${next_remain_date}'"
execSqlOnMysql "${update_next_remain_sql}"


update_third_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT new_date,remain_count,remain_type
  FROM $tmp_mysql_remain_table
  WHERE new_date='${third_remain_date}' AND remain_type='3'
) b
ON a.stat_date=b.new_date
SET a.short_video_user_third_remain=b.remain_count
WHERE a.stat_date='${third_remain_date}'"
execSqlOnMysql "${update_third_remain_sql}"


update_seven_remain_sql="UPDATE $mysql_table a
JOIN (
  SELECT new_date,remain_count,remain_type
  FROM $tmp_mysql_remain_table
  WHERE new_date='${seven_remain_date}' AND remain_type='7'
) b
ON a.stat_date=b.new_date
SET a.short_video_user_seven_remain=b.remain_count
WHERE a.stat_date='${seven_remain_date}'"
execSqlOnMysql "${update_seven_remain_sql}"

echo "############### 抖音视频用户留存数据统计 end #####################"

