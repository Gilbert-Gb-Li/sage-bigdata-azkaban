#!/bin/sh
#source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
#source ${base_path}/util.sh

today=$1
dayBeforeYesterday=`date -d "-2 day $today" +%Y-%m-%d`
threeDaysAgo=`date -d "-3 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive -e "use bigdata;${COMMON_VAR}
insert into bigdata.advert_douyin_user_count_record_daily_snapshot partition(dt='${dayBeforeYesterday}')
select count(distinct user_id)
from douyin_user_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(user_id) cn_valid_user
from advert_douyin_valid_user_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(b.user_id)
from (select user_id from advert_douyin_valid_user_daily_snapshot where dt='${dayBeforeYesterday}') b 
left outer join (select user_id from advert_douyin_valid_user_daily_snapshot where dt='${threeDaysAgo}') a 
on b.user_id=a.user_id where a.user_id is null
union all
select count(distinct short_video_id)     
from douyin_video_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(short_video_id)
from advert_douyin_valid_vedio_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(b.short_video_id)
from (select short_video_id from advert_douyin_valid_vedio_daily_snapshot where dt='${dayBeforeYesterday}') b 
left outer join (select short_video_id from advert_douyin_valid_vedio_daily_snapshot where dt='${threeDaysAgo}') a 
on b.short_video_id=a.short_video_id where a.short_video_id is null
union all
select count(distinct challenge_id)
from douyin_challenge_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(challenge_id) from advert_douyin_valid_topic_daily_snapshot 
where dt='${dayBeforeYesterday}'
union all
select count(b.challenge_id)
from (select challenge_id from advert_douyin_valid_topic_daily_snapshot where dt='${dayBeforeYesterday}') b 
left outer join (select challenge_id from advert_douyin_valid_topic_daily_snapshot where dt='${threeDaysAgo}') a 
on b.challenge_id=a.challenge_id where a.challenge_id is null
union all 
select count(distinct comment_id)
from douyin_video_comment_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(comment_id) 
from advert_douyin_valid_comment_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(distinct user_id)
from douyin_user_daily_snapshot
where dt='${dayBeforeYesterday}' and follower_count>=8000
union all
select count(kol_id) 
from bigdata.advert_douyin_valid_kol_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(b.kol_id)
from (select kol_id from advert_douyin_valid_kol_daily_snapshot where dt='${dayBeforeYesterday}') b 
left outer join (select kol_id from advert_douyin_valid_kol_daily_snapshot where dt='${threeDaysAgo}') a 
on b.kol_id=a.kol_id where a.kol_id is null
union all
select count(a.short_video_id)
from (select distinct author_id,short_video_id  
      from douyin_video_daily_snapshot
      where dt='${dayBeforeYesterday}') a 
join (select distinct user_id 
      from douyin_user_daily_snapshot 
      where dt='${dayBeforeYesterday}' and follower_count>=8000) b
on a.author_id=b.user_id
union all
select count(short_video_id)
from advert_douyin_kol_valid_vedio_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(b.short_video_id)
from (select short_video_id from advert_douyin_kol_valid_vedio_daily_snapshot where dt='${dayBeforeYesterday}') b 
left outer join (select short_video_id from advert_douyin_kol_valid_vedio_daily_snapshot where dt='${threeDaysAgo}') a 
on b.short_video_id=a.short_video_id where a.short_video_id is null
union all
select count(distinct a.challenge_id)
from (select distinct author_id,challenge_id
      from douyin_video_daily_snapshot
      where dt='${dayBeforeYesterday}') a 
join (select distinct user_id 
      from douyin_user_daily_snapshot 
      where dt='${dayBeforeYesterday}' and follower_count>=8000) b
on a.author_id=b.user_id
union all
select count(challenge_id) 
from advert_douyin_valid_topic_kol_daily_snapshot
where dt='${dayBeforeYesterday}'
union all
select count(b.challenge_id)
from (select challenge_id from advert_douyin_valid_topic_kol_daily_snapshot where dt='${dayBeforeYesterday}') b 
left outer join (select challenge_id from advert_douyin_valid_topic_kol_daily_snapshot where dt='${threeDaysAgo}') a 
on b.challenge_id=a.challenge_id where a.challenge_id is null
union all
select count(distinct c.comment_id)
from (select distinct short_video_id,comment_id
      from douyin_video_comment_daily_snapshot
      where dt='${dayBeforeYesterday}') c
join (select a.short_video_id
      from (select distinct author_id,short_video_id  
            from douyin_video_daily_snapshot
            where dt='${dayBeforeYesterday}') a 
      join (select distinct user_id 
            from douyin_user_daily_snapshot 
            where dt='${dayBeforeYesterday}' and follower_count>=8000) b
      on a.author_id=b.user_id) d
on c.short_video_id=d.short_video_id
union all
select count(comment_id) 
from advert_douyin_valid_comment_kol_daily_snapshot
where dt='${dayBeforeYesterday}';"

echo "执行完成"
