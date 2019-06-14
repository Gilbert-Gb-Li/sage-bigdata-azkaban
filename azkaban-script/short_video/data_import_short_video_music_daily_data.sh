#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
echo "${date}"
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
echo "${yesterday}"

echo "============= data import tbl_ex_short_video_music_daily_snapshot start ====================="
hive_sql="insert into short_video.tbl_ex_short_video_music_daily_snapshot partition(dt='${date}')
select k.music_id,k.music_name,k.music_is_original,k.music_author,k.music_user_count,
case when (j.music_user_count is null or cast(j.music_user_count as bigint)<0) then k.music_user_count when cast(j.music_user_count as bigint)>0 then cast(k.music_user_count as bigint)-cast(j.music_user_count as bigint) end music_new_user_count,
k.music_play_url
from
(select g.music_id,g.music_name,g.music_is_original,g.music_author,g.music_count as music_user_count,g.music_play_url
from
(select t.music_id,t.music_name,t.music_is_original,t.music_author,t.music_play_url,t.tag,y.music_count,row_number() over(partition by t.music_id order by t.tag) as rank
from
(select music_id,music_name,music_is_original,music_author,music_play_url,case when music_is_original='true' then 1 when music_is_original='false' then 2 when (music_is_original<>'false' or music_is_original<>'true') then 3 end tag
from short_video.tbl_ex_short_video_data_daily_snapshot where music_id is not null and music_id<>'' and dt='${date}'
) as t
left join
(select music_id,count(music_id) as music_count from short_video.tbl_ex_short_video_data_daily_snapshot where music_id is not null and music_id<>'' and dt='${date}' group by music_id
) as y
on (t.music_id=y.music_id)
) as g where g.rank=1
) as k
left join
(select music_id,music_user_count from short_video.tbl_ex_short_video_music_daily_snapshot where dt='${yesterday}'
) as j
on (k.music_id=j.music_id)"
echo "${hive_sql}"

executeHiveCommand "${hive_sql}"
echo "============= data import tbl_ex_short_video_music_daily_snapshot end ====================="