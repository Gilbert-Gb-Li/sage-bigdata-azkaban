#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into bigdata.douyin_music_daily_snapshot partition(dt='${date}')
select a.record_time,a.music_use_count,a.music_name,a.music_id,b.music_video_count,a.music_play_url,a.music_author from
(select record_time,music_id,music_name,music_use_count,music_play_url,music_author from
(select *,row_number() over (partition by music_id order by record_time desc) as order_num from (
select t1.record_time,t1.music_id,t1.music_name,t1.music_use_count,t1.music_play_url,case when t1.music_author is null or t1.music_author = '' then t2.music_author else t1.music_author end music_author from
(select a.record_time,a.music_id,a.music_name,a.music_use_count,case when a.music_play_url is null or a.music_play_url = '' then b.music_play_url else a.music_play_url end music_play_url,a.music_author from
(select record_time,music_id,music_name,music_use_count,music_play_url,music_author from bigdata.douyin_music_data_origin_orc where dt = '${date}'
union all
select record_time,music_id,music_name,music_use_count,music_play_url,music_author from bigdata.douyin_hot_music_data_origin_orc where dt = '${date}') a
left join
(select music_id,music_play_url from
(select *,row_number() over (partition by music_id order by record_time desc) as order_num from
(select record_time,music_id,music_play_url from bigdata.douyin_hot_music_data_origin_orc where dt = '${date}' and music_play_url != '' and music_play_url is not null) a) t
where t.order_num = 1) b
on a.music_id = b.music_id) t1
left join
(select music_id,music_author from
(select *,row_number() over (partition by music_id order by record_time desc) as order_num from
(select record_time,music_id,music_author from bigdata.douyin_hot_music_data_origin_orc where dt = '${date}' and music_author != '' and music_author is not null) a) t
where t.order_num = 1) t2
on t1.music_id = t2.music_id
union all
select record_time,music_id,music_name,music_use_count,music_play_url,music_author from bigdata.douyin_music_daily_snapshot where dt = '${yesterday}'
)as p)as t
where t.order_num =1) a
left join
(select music_id,count(distinct short_video_id) as music_video_count from bigdata.douyin_video_daily_snapshot where dt = '${date}' and music_id != '' and short_video_id != '' group by music_id) b
on a.music_id = b.music_id;"

executeHiveCommand "${hive_sql}"