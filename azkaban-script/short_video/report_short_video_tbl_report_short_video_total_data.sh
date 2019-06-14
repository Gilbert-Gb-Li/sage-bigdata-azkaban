#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 抖音累计报表统计 start #####################"
date=$1

mysql_table="tbl_report_short_video_total_data"

hive_sql="select '${date}', max(video_count) as video_count,max(video_user_count) as video_user_count, max(comment_count) as comment_count,
max(comment_user_count) as comment_user_count,max(user_count) as user_count from (
select count(distinct short_video_id) as video_count, 0 as video_user_count, 0 as comment_count, 0 as comment_user_count, 0 as user_count
from ias.tbl_ex_short_video_data_origin_orc 
union all
select 0 as video_count, count(1) as video_user_count, 0 as comment_count, 0 as comment_user_count , 0 as user_count
from (select author_id,count(distinct short_video_id) as video_count from  ias.tbl_ex_short_video_data_origin_orc group by author_id) as t
where t.video_count >=5
union all 
select 0 as video_count, 0 as video_user_count, count(distinct comment_id) as comment_count, count(distinct user_id) as comment_user_count , 0 as user_count
from ias.tbl_ex_short_video_comment_origin_orc 
union all
select 0 as video_count, 0 as video_user_count, 0 as comment_count, 0 as comment_user_count, count(distinct user_id) as user_count
from ias.tbl_ex_short_video_user_origin_orc
) as t"

hiveSqlToMysql "${hive_sql}" "${date}" "${mysql_table}" "stat_date,video_count,video_user_count,comment_count,comment_user_count,user_count" "stat_date"

echo "############### 抖音累计报表统计 end #####################"