#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

echo "${date}"

echo "============= data import tbl_ex_short_video_comment_rank_daily_data start====================="
hive_sql_1="insert into short_video.tbl_ex_short_video_comment_rank_daily_data partition(dt='${date}')
select short_video_id,from_unixtime(cast(substr(video_create_time,0,10) as bigint),'yyyy-MM-dd') as create_date,description,comment_count,nick_name
from short_video.tbl_ex_short_video_data_daily_snapshot 
where dt='${date}'
order by cast(comment_count as bigint) desc 
limit 10000"

executeHiveCommand "${hive_sql_1}"
echo "============= data import tbl_ex_short_video_comment_rank_daily_data END====================="


echo "============= data import tbl_ex_short_video_share_rank_daily_data start====================="
hive_sql_2="insert into short_video.tbl_ex_short_video_share_rank_daily_data partition(dt='${date}')
select short_video_id,from_unixtime(cast(substr(video_create_time,0,10) as bigint),'yyyy-MM-dd') as create_date,description,share_count,nick_name
from short_video.tbl_ex_short_video_data_daily_snapshot 
where dt='${date}'
order by cast(share_count as bigint) desc 
limit 10000"

executeHiveCommand "${hive_sql_2}"
echo "============= data import tbl_ex_short_video_share_rank_daily_data END====================="



echo "============= data import tbl_ex_short_video_like_rank_daily_data start====================="
hive_sql_3="insert into short_video.tbl_ex_short_video_like_rank_daily_data partition(dt='${date}')
select short_video_id,from_unixtime(cast(substr(video_create_time,0,10) as bigint),'yyyy-MM-dd') as create_date,description,like_count,nick_name
from short_video.tbl_ex_short_video_data_daily_snapshot 
where dt='${date}'
order by cast(like_count as bigint) desc 
limit 10000"

executeHiveCommand "${hive_sql_3}"
echo "============= data import tbl_ex_short_video_like_rank_daily_data start====================="

echo "============= data import tbl_ex_short_video_play_rank_daily_data start====================="
hive_sql_4="insert into short_video.tbl_ex_short_video_play_rank_daily_data partition(dt='${date}')
select short_video_id,from_unixtime(cast(substr(video_create_time,0,10) as bigint),'yyyy-MM-dd') as create_date,description,play_count,nick_name
from short_video.tbl_ex_short_video_data_daily_snapshot 
where dt='${date}'
order by cast(play_count as bigint) desc 
limit 10000"

executeHiveCommand "${hive_sql_4}"
echo "============= data import tbl_ex_short_video_play_rank_daily_data start====================="


echo "============= data import tbl_ex_short_video_all_rank_daily_data start====================="
hive_sql_4="insert into short_video.tbl_ex_short_video_all_rank_daily_data partition(dt='${date}')
select short_video_id,from_unixtime(cast(substr(video_create_time,0,10) as bigint),'yyyy-MM-dd') as create_date,description,
cast((share_count+comment_count+like_count+play_count) as bigint) as rank_count,nick_name
from short_video.tbl_ex_short_video_data_daily_snapshot 
where dt='${date}'
order by cast(rank_count as bigint) desc 
limit 10000"

executeHiveCommand "${hive_sql_4}"
echo "============= data import tbl_ex_short_video_all_rank_daily_data start====================="

