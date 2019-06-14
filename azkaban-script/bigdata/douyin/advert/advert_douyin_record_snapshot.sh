#!/bin/sh
#source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
#source ${base_path}/util.sh

today=$1
dayBeforeYesterday=`date -d "-2 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive -e "use bigdata;${COMMON_VAR}
insert into bigdata.advert_douyin_valid_user_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct user_id
from (select distinct user_id,short_video_count 
      from douyin_user_daily_snapshot
      where dt='${dayBeforeYesterday}') b
join (select a.author_id author_id,count(a.author_id) author_id_count
      from (select distinct author_id,short_video_id
            from douyin_video_daily_snapshot   
            where short_video_id is not null and dt='${dayBeforeYesterday}'
            ) a
      group by a.author_id
      ) c
on b.user_id=c.author_id and b.short_video_count=c.author_id_count;
insert into bigdata.advert_douyin_valid_vedio_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct short_video_id
from (select distinct author_id,short_video_id
      from douyin_video_daily_snapshot
      where dt='${dayBeforeYesterday}') a
join (select user_id 
      from advert_douyin_valid_user_daily_snapshot 
      where dt='${dayBeforeYesterday}') b
on a.author_id=b.user_id and b.user_id is not null;
insert into bigdata.advert_douyin_valid_topic_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct a.challenge_id
from (select challenge_id 
      from douyin_challenge_daily_snapshot 
      where dt='${dayBeforeYesterday}') a
join (select c.challenge_id,s.short_video_id
      from (select short_video_id 
            from advert_douyin_valid_vedio_daily_snapshot 
            where dt='${dayBeforeYesterday}') s
      left join (select distinct challenge_id,short_video_id 
                 from douyin_video_daily_snapshot 
                 where dt='${dayBeforeYesterday}' and challenge_id is not null) c
      on s.short_video_id=c.short_video_id) b
on a.challenge_id=b.challenge_id;
insert into bigdata.advert_douyin_valid_comment_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct aa.comment_id
      from (select distinct short_video_id,comment_id 
            from douyin_video_comment_daily_snapshot 
            where dt='${dayBeforeYesterday}' and comment_id is not null) aa
      join (select short_video_id 
            from advert_douyin_valid_vedio_daily_snapshot 
            where dt='${dayBeforeYesterday}') bb
on aa.short_video_id=bb.short_video_id;
insert into bigdata.advert_douyin_valid_kol_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct a.user_id
from (select user_id 
      from douyin_user_daily_snapshot 
      where dt='${dayBeforeYesterday}' and follower_count>=8000) a
join (select user_id
     from advert_douyin_valid_user_daily_snapshot
     where dt='${dayBeforeYesterday}') b
on a.user_id=b.user_id;
insert into bigdata.advert_douyin_kol_valid_vedio_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct c.short_video_id 
from (select short_video_id 
      from advert_douyin_valid_vedio_daily_snapshot 
      where dt='${dayBeforeYesterday}') c
join (select a.short_video_id
      from (select distinct author_id,short_video_id  
            from douyin_video_daily_snapshot
            where dt='${dayBeforeYesterday}') a 
      join (select distinct user_id 
            from douyin_user_daily_snapshot 
            where dt='${dayBeforeYesterday}' and follower_count>=8000) b
      on a.author_id=b.user_id) d
on c.short_video_id=d.short_video_id;
insert into bigdata.advert_douyin_valid_topic_kol_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct a.challenge_id
from (select distinct author_id,challenge_id,short_video_id
      from douyin_video_daily_snapshot
      where dt='${dayBeforeYesterday}') a 
join (select distinct user_id 
      from douyin_user_daily_snapshot 
      where dt='${dayBeforeYesterday}' and follower_count>=8000) b
on a.author_id=b.user_id
join (select short_video_id 
      from advert_douyin_kol_valid_vedio_daily_snapshot
      where dt='${dayBeforeYesterday}') c
on a.short_video_id=c.short_video_id;
insert into bigdata.advert_douyin_valid_comment_kol_daily_snapshot partition(dt='${dayBeforeYesterday}')
select distinct a.comment_id
from (select distinct short_video_id,comment_id
      from douyin_video_comment_daily_snapshot
      where dt='${dayBeforeYesterday}') a
join (select short_video_id
      from advert_douyin_kol_valid_vedio_daily_snapshot
      where dt='${dayBeforeYesterday}') b
on a.short_video_id=b.short_video_id;"
echo "执行完成"
