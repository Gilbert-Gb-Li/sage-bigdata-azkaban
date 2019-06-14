#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
date_reduce_1=`date -d "-1 day $yesterday" +%Y-%m-%d`
stat_date=`date -d "$yesterday" +%Y%m%d`
year=`date -d "$yesterday" +%Y`
month=`date -d "$yesterday" +%m`

echo "++++++++++++++++++++++++++++++++计算生成头部集视频数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql1="insert into bigdata.douyin_video_r_t_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'video' as meta_table_name,t1.record_time,'1' as set_type,t1.short_video_id,t1.author_id,t1.music_id,t1.challenge_name,t1.challenge_id,t1.location,t1.hot_id,case when t3.hot_name is null then '' else t3.hot_name end hot_name,t1.is_advert,
t1.like_count,case when t2.like_count is null or t2.like_count = '' or t2.like_count = -1 then t1.like_count else t1.like_count - t2.like_count end new_like_count,
t1.comments_count,case when t2.comments_count is null or t2.comments_count = '' or t2.comments_count = -1 then t1.comments_count else t1.comments_count - t2.comments_count end new_comments_count,
t1.share_count,case when t2.share_count is null or t2.share_count = '' or t2.share_count = -1 then t1.share_count else t1.share_count - t2.share_count end new_share_count from
(select a.record_time,a.short_video_id,a.author_id,a.is_advert,a.music_id,a.challenge_id,a.challenge_name,a.location,a.hot_id,a.like_count,a.comments_count,a.share_count from
(select record_time,short_video_id,author_id,is_advert,music_id,challenge_id,challenge_name,location,hot_id,like_count,comments_count,share_count from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.author_id = b.user_id) t1
left join
(select a.short_video_id,a.author_id,a.like_count,a.comments_count,a.share_count from
(select short_video_id,author_id,like_count,comments_count,share_count from bigdata.douyin_video_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.author_id = b.user_id) t2
on t1.short_video_id = t2.short_video_id
left join
(select hot_id,hot_name from bigdata.douyin_hot_recommend_details_daily_snapshot where dt = '${yesterday}') as t3
on t1.hot_id = t3.hot_id;"

executeHiveCommand "${hive_sql1}"

echo "++++++++++++++++++++++++++++++++计算生成抽样集视频数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql2="insert into bigdata.douyin_video_r_t_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'video' as meta_table_name,t1.record_time,'2' as set_type,t1.short_video_id,t1.author_id,t1.music_id,t1.challenge_name,t1.challenge_id,t1.location,t1.hot_id,case when t3.hot_name is null then '' else t3.hot_name end hot_name,t1.is_advert,
t1.like_count,case when t2.like_count is null or t2.like_count = '' or t2.like_count = -1 then t1.like_count else t1.like_count - t2.like_count end new_like_count,
t1.comments_count,case when t2.comments_count is null or t2.comments_count = '' or t2.comments_count = -1 then t1.comments_count else t1.comments_count - t2.comments_count end new_comments_count,
t1.share_count,case when t2.share_count is null or t2.share_count = '' or t2.share_count = '' then t1.share_count else t1.share_count - t2.share_count end new_share_count from
(select a.record_time,a.short_video_id,a.author_id,a.is_advert,a.music_id,a.challenge_id,a.challenge_name,a.location,a.hot_id,a.like_count,a.comments_count,a.share_count from
(select record_time,short_video_id,author_id,is_advert,music_id,challenge_id,challenge_name,location,hot_id,like_count,comments_count,share_count from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${year}-${month}-01') b
on a.author_id = b.user_id) t1
left join
(select a.short_video_id,a.author_id,a.like_count,a.comments_count,a.share_count from
(select short_video_id,author_id,like_count,comments_count,share_count from bigdata.douyin_video_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${year}-${month}-01') b
on a.author_id = b.user_id) t2
on t1.short_video_id = t2.short_video_id
left join
(select hot_id,hot_name from bigdata.douyin_hot_recommend_details_daily_snapshot where dt = '${yesterday}') as t3
on t1.hot_id = t3.hot_id;"

executeHiveCommand "${hive_sql2}"

echo "++++++++++++++++++++++++++++++++导出视频数据到ES++++++++++++++++++++++++++++++++++++++"
hive_sql3="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;insert into bigdata.douyin_video_r_t_es_data partition(dt='${yesterday}')
select '${stat_date}',unix_timestamp(dt, 'yyyy-MM-dd')*1000,meta_app_name,meta_table_name,record_time,set_type,short_video_id,author_id,music_id,challenge_name,challenge_id,location,hot_id,hot_name,is_advert,like_count,new_like_count,comments_count,new_comments_count,share_count,new_share_count
from bigdata.douyin_video_r_t_data where dt = '${yesterday}'"
executeHiveCommand "${hive_sql3}"