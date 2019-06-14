#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into short_video.tbl_ex_short_video_data_daily_snapshot partition(dt='${date}')
select t1.*,t2.nick_name from (
select record_time,short_video_id,author_id,video_create_time,comment_count,share_count,play_count,like_count,description,
share_url,cover_url_list,play_url_list,download_url_list,music_id, music_play_url,music_is_original,music_author,
music_name,shallenge_id,challenge_name
from
(select *,row_number() over (partition by short_video_id order by record_time desc) as order_num from (
select record_time,short_video_id,author_id,video_create_time,comment_count,share_count,play_count,like_count,description,
share_url,cover_url_list,play_url_list,download_url_list,music_id, music_play_url,music_is_original,music_author,
music_name,shallenge_id,challenge_name 
from ias.tbl_ex_short_video_data_origin_orc 
where dt='${date}'
union all
select record_time,short_video_id,author_id,video_create_time,comment_count,share_count,play_count,like_count,description,
share_url,cover_url_list,play_url_list,download_url_list,music_id, music_play_url,music_is_original,music_author,
music_name,shallenge_id,challenge_name
from short_video.tbl_ex_short_video_data_daily_snapshot 
where dt='${yesterday}'
)as p
)as t
where t.order_num =1) as t1 
left join (
select user_id,nick_name from short_video.tbl_ex_short_video_user_daily_snapshot where dt='${date}'
)as t2 on t1.author_id=t2.user_id"

executeHiveCommand "${hive_sql}"