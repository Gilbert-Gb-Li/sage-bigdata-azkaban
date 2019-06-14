#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
hive_sql="insert into short_video.tbl_ex_short_video_user_location_daily_snapshot partition(dt='${date}')
select h.record_time,h.user_id,h.internal_uid,h.country,h.province,h.city,h.longitude,h.latitude
from
(select *,row_number() over (partition by user_id order by record_time desc) as order_num
from
(select e.record_time,e.user_id,e.internal_uid,
case when f.country is null then e.province else f.country end country,
case when f.country is null then null else e.province end province,
e.city,e.longitude,e.latitude
from
(select c.record_time,c.user_id,c.internal_uid,
case when d.province is null then c.province else d.province end province,
case when d.city is null then c.city else d.city end city,
d.longitude,d.latitude
from
(select b.record_time,b.user_id,b.internal_uid,
case when b.user_location like concat('%','·','%') then split(b.user_location,'·')[0] else null end province,
case when b.user_location like concat('%','·','%') then split(b.user_location,'·')[1] else user_location end city
from
(select record_time,user_id,user_location,internal_uid,row_number () over (partition by user_id order by record_time desc) as rank from ias.tbl_ex_short_video_user_origin_orc where dt='${date}' and user_location is not null and user_location <> ''
) as b
where b.rank=1
) as c
left join
(select country,province,city,longitude,latitude from short_video.tbl_ex_world_city
) as d
on c.city=d.city
) as e
left join
(select distinct country,province from short_video.tbl_ex_world_city
) as f
on e.province=f.province
union all
select record_time,user_id,internal_uid,country,province,city,longitude,latitude
from short_video.tbl_ex_short_video_user_location_daily_snapshot
where dt='${yesterday}'
) as g
) as h
where h.order_num =1"


executeHiveCommand "${hive_sql}"
