#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
date_reduce_1=`date -d "-1 day $yesterday" +%Y-%m-%d`
stat_date=`date -d "$yesterday" +%Y%m%d`
year=`date -d "$yesterday" +%Y`
month=`date -d "$yesterday" +%m`

echo "++++++++++++++++++++++++++++++++计算生成全量集视频数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql1="insert into bigdata.douyin_video_all_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'video' as meta_table_name,t1.record_time,t1.short_video_id,t1.author_id,t1.music_id,t1.challenge_name,t1.challenge_id,t1.location,t1.hot_id,case when t3.hot_name is null then '' else t3.hot_name end hot_name,t1.is_advert,
t1.like_count,case when t2.like_count is null or t2.like_count = '' or t2.like_count = -1 then t1.like_count else t1.like_count - t2.like_count end new_like_count,
t1.comments_count,case when t2.comments_count is null or t2.comments_count = '' or t2.comments_count = -1 then t1.comments_count else t1.comments_count - t2.comments_count end new_comments_count,
t1.share_count,case when t2.share_count is null or t2.share_count = '' or t2.share_count = -1 then t1.share_count else t1.share_count - t2.share_count end new_share_count from
(select record_time,short_video_id,author_id,is_advert,music_id,challenge_id,challenge_name,location,hot_id,like_count,comments_count,share_count from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}') t1
left join
(select short_video_id,author_id,like_count,comments_count,share_count from bigdata.douyin_video_daily_snapshot where dt = '${year}-${month}-01') t2
on t1.short_video_id = t2.short_video_id
left join
(select hot_id,hot_name from bigdata.douyin_hot_recommend_details_daily_snapshot where dt = '${yesterday}') as t3
on t1.hot_id = t3.hot_id;"

executeHiveCommand "${hive_sql1}"

echo "++++++++++++++++++++++++++++++++导出全量集视频数据到ES++++++++++++++++++++++++++++++++++++++"
hive_sql2="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;insert into bigdata.douyin_video_all_es_data partition(dt='${yesterday}')
select substr(${stat_date},1,6) as stat_month,unix_timestamp(dt, 'yyyy-MM-dd')*1000,meta_app_name,meta_table_name,record_time,short_video_id,author_id,music_id,challenge_name,challenge_id,location,hot_id,hot_name,is_advert,like_count,new_like_count,comments_count,new_comments_count,share_count,new_share_count
from bigdata.douyin_video_all_data where dt = '${yesterday}'"
executeHiveCommand "${hive_sql2}"