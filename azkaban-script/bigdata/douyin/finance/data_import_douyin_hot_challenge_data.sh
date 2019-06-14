#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
stat_date=`date -d "$yesterday" +%Y%m%d`

echo "++++++++++++++++++++++++++++++++计算生成热门话题数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql1="insert into bigdata.douyin_hot_challenge_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'hot_challenge' as meta_table_name,t1.challenge_count,case when t2.total_play_count is null then 0 else t2.total_play_count end total_play_count,
case when t2.total_video_count is null then 0 else t2.total_video_count end total_video_count from
(select '${yesterday}' as stat_date,count(distinct challenge_id) as challenge_count from bigdata.douyin_hot_challenge_daily_snapshot where dt = '${yesterday}') t1
left join
(select '${yesterday}' as stat_date,sum(challenge_play_count) as total_play_count,sum(challenge_video_count) as total_video_count from bigdata.douyin_hot_challenge_daily_snapshot where dt = '${yesterday}') t2
on t1.stat_date=t2.stat_date;"

executeHiveCommand "${hive_sql1}"

echo "++++++++++++++++++++++++++++++++导出用户数据到ES++++++++++++++++++++++++++++++++++++++"
hive_sql2="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;insert into bigdata.douyin_hot_challenge_es_data partition(dt='${yesterday}')
select '${stat_date}',unix_timestamp(dt, 'yyyy-MM-dd')*1000,meta_app_name,meta_table_name,challenge_count,challenge_play_count,challenge_video_count 
from bigdata.douyin_hot_challenge_data where dt = '${yesterday}'"
executeHiveCommand "${hive_sql2}"