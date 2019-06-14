#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into bigdata.douyin_challenge_daily_snapshot partition(dt='${date}')
select a.record_time,a.challenge_look_count,a.challenge_name,a.challenge_id,b.challenge_video_count,a.challenge_play_count,a.challenge_desc,a.challenge_author from
(select record_time,challenge_id,challenge_name,challenge_look_count,challenge_play_count,challenge_desc,challenge_author from
(select *,row_number() over (partition by challenge_id order by record_time desc) as order_num from (
select t1.record_time,t1.challenge_id,t1.challenge_name,t1.challenge_look_count,t1.challenge_play_count,t1.challenge_desc,case when t1.challenge_author is null or t1.challenge_author = '' then t2.challenge_author else t1.challenge_author end challenge_author from
(select a.record_time,a.challenge_id,a.challenge_name,case when a.challenge_look_count is null or a.challenge_look_count = '' then b.challenge_look_count else a.challenge_look_count end challenge_look_count,
a.challenge_play_count,a.challenge_desc,a.challenge_author from
(select record_time,challenge_id,challenge_name,challenge_look_count,challenge_play_count,challenge_desc,challenge_author from bigdata.douyin_challenge_data_origin_orc where dt = '${date}'
union all
select record_time,challenge_id,challenge_name,challenge_look_count,challenge_play_count,challenge_desc,challenge_author from bigdata.douyin_hot_challenge_data_origin_orc where dt = '${date}') a
left join
(select challenge_id,challenge_look_count from
(select *,row_number() over (partition by challenge_id order by record_time desc) as order_num from
(select record_time,challenge_id,challenge_look_count from bigdata.douyin_challenge_data_origin_orc where dt = '${date}' and challenge_look_count is not null and challenge_look_count != ''
union all
select record_time,challenge_id,challenge_look_count from bigdata.douyin_hot_challenge_data_origin_orc where dt = '${date}' and challenge_look_count is not null and challenge_look_count != '') a)t
where t.order_num = 1) b
on a.challenge_id = b.challenge_id) t1
left join
(select challenge_id,challenge_author from
(select *,row_number() over (partition by challenge_id order by record_time desc) as order_num from
(select record_time,challenge_id,challenge_author from bigdata.douyin_challenge_data_origin_orc where dt = '${date}' and challenge_author is not null and challenge_author != '') a)t
where t.order_num = 1) t2
on t1.challenge_id = t2.challenge_id
union all
select record_time,challenge_id,challenge_name,challenge_look_count,challenge_play_count,challenge_desc,challenge_author from bigdata.douyin_challenge_daily_snapshot where dt = '${yesterday}'
)as p)as t
where t.order_num =1) a
left join
(select challenge_id,count(distinct short_video_id) as challenge_video_count from bigdata.douyin_video_daily_snapshot where dt = '${date}' and challenge_id != '' and short_video_id != '' group by challenge_id) b
on a.challenge_id = b.challenge_id;"

executeHiveCommand "${hive_sql}"