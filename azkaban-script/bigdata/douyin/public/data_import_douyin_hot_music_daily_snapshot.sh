#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into bigdata.douyin_hot_music_daily_snapshot partition(dt='${date}')
select a.record_time,a.music_use_count,a.music_name,a.music_id,b.music_video_count,a.music_play_url,a.music_author from
(select record_time,music_id,music_name,music_use_count,music_play_url,music_author from
(select *,row_number() over (partition by music_id order by record_time desc) as order_num from (
select record_time,music_id,music_name,music_use_count,music_play_url,music_author from bigdata.douyin_hot_music_data_origin_orc where dt = '${date}'
union all
select record_time,music_id,music_name,music_use_count,music_play_url,music_author from bigdata.douyin_hot_music_daily_snapshot where dt = '${yesterday}'
)as p)as t
where t.order_num =1) a
left join
(select music_id,count(distinct short_video_id) as music_video_count from bigdata.douyin_video_daily_snapshot where dt = '${date}' and music_id != '' and short_video_id != '' group by music_id) b
on a.music_id = b.music_id;"

executeHiveCommand "${hive_sql}"