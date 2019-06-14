#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
echo "${date}"
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
echo "${yesterday}"

echo "============= data import tbl_ex_short_video_challenge_daily_snapshot start ====================="
hive_sql="insert into short_video.tbl_ex_short_video_challenge_daily_snapshot partition(dt='${date}')
select f.challenge_id,f.challenge_name,f.challenge_user_count,f.challenge_description,
case when f.yesterday_count is null then f.challenge_user_count when cast(f.yesterday_count as bigint)>0 then cast(f.challenge_user_count as bigint)-cast(f.yesterday_count as bigint) end challenge_new_user_count
from(
select d.challenge_id,d.challenge_name,d.challenge_user_count,'' as challenge_description,e.yesterday_count,row_number() over(partition by d.challenge_id order by cast(d.challenge_user_count as bigint) desc) as rank_two
from
(select c.challenge_id,c.challenge_name,c.challenge_user_count from (
select a.shallenge_id as challenge_id,b.challenge_name,a.video_count as challenge_user_count,row_number() over(partition by a.shallenge_id order by cast(a.video_count as bigint) desc) as rank
from(select shallenge_id,count(shallenge_id) as video_count from short_video.tbl_ex_short_video_data_daily_snapshot where shallenge_id is not null and shallenge_id<>'' and dt='${date}' group by shallenge_id) as a
left join(select shallenge_id,challenge_name from short_video.tbl_ex_short_video_data_daily_snapshot where dt='${date}') as b on (a.shallenge_id=b.shallenge_id)) as c where c.rank=1
) as d
left join
(select challenge_id,challenge_user_count as yesterday_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt='${yesterday}'
) as e
on (d.challenge_id=e.challenge_id)) as f where f.rank_two=1"
echo "${hive_sql}"

executeHiveCommand "${hive_sql}"
echo "============= data import tbl_ex_short_video_challenge_daily_snapshot end ====================="