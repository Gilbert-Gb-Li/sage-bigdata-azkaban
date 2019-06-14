#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
#source ${base_path}/util.sh

today=$1
yesterday=`date -d "-0 day $today" +%Y-%m-%d`
dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
table_path='/data/douyin/advert/snapshot/advert_douyin_user_count_record_daily_second_snapshot'
hdfs dfs -rm ${table_path}/dt=$yesterday/*
hdfs dfs -rmdir ${table_path}/dt=$yesterday
hive -e "use bigdata;
ALTER TABLE bigdata.advert_douyin_user_count_record_daily_second_snapshot DROP IF EXISTS PARTITION(dt='${yesterday}');
insert overwrite table bigdata.advert_douyin_user_count_record_daily_second_snapshot partition(dt='${yesterday}')
select count(distinct user_id)
from douyin_user_daily_snapshot
where dt='${yesterday}'
union all
select count(user_id) cn_valid_user
from advert_douyin_valid_user_daily_snapshot
where dt='${yesterday}'
union all
select count(b.user_id)
from (select user_id from advert_douyin_valid_user_daily_snapshot where dt='${yesterday}') b
left outer join (select user_id from advert_douyin_valid_user_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.user_id=a.user_id where a.user_id is null
union all
select count(distinct short_video_id)
from douyin_video_daily_snapshot
where dt='${yesterday}'
union all
select count(short_video_id)
from advert_douyin_valid_vedio_daily_snapshot
where dt='${yesterday}'
union all
select count(b.short_video_id)
from (select short_video_id from advert_douyin_valid_vedio_daily_snapshot where dt='${yesterday}') b
left outer join (select short_video_id from advert_douyin_valid_vedio_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.short_video_id=a.short_video_id where a.short_video_id is null
union all
select count(distinct challenge_id)
from douyin_challenge_daily_snapshot
where dt='${yesterday}'
union all
select count(challenge_id)
from advert_douyin_valid_topic_daily_snapshot
where dt='${yesterday}'
union all
select count(b.challenge_id)
from (select challenge_id from advert_douyin_valid_topic_daily_snapshot where dt='${yesterday}') b
left outer join (select challenge_id from advert_douyin_valid_topic_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.challenge_id=a.challenge_id where a.challenge_id is null
union all
select count(distinct comment_id)
from douyin_video_comment_daily_snapshot
where dt='${yesterday}'
union all
select count(comment_id)
from advert_douyin_valid_comment_daily_snapshot
where dt='${yesterday}'
union all
select count(b.comment_id)
from (select comment_id from advert_douyin_valid_comment_daily_snapshot where dt='${yesterday}') b
left outer join (select comment_id from advert_douyin_valid_comment_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.comment_id=a.comment_id where a.comment_id is null
union all
select count(b.user_id)
from (select user_id from douyin_advert_kol_data_snapshot where dt='${yesterday}') b
left outer join (select user_id from douyin_advert_kol_data_snapshot where dt='${dayBeforeYesterday}') a
on b.user_id=a.user_id where a.user_id is null
union all
select count(user_id)
from douyin_advert_kol_data_snapshot
where dt='${yesterday}'
union all
select count(kol_id)
from bigdata.advert_douyin_valid_kol_daily_snapshot
where dt='${yesterday}'
union all
select count(b.kol_id)
from (select kol_id from advert_douyin_valid_kol_daily_snapshot where dt='${yesterday}') b
left outer join (select kol_id from advert_douyin_valid_kol_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.kol_id=a.kol_id where a.kol_id is null
union all
select count(b.short_video_id)
from (select short_video_id from advert_douyin_valid_kol_vedio_daily_snapshot where dt='${yesterday}') b
left outer join (select short_video_id from advert_douyin_valid_kol_vedio_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.short_video_id=a.short_video_id where a.short_video_id is null
union all
select count(short_video_id)
from advert_douyin_valid_kol_vedio_daily_snapshot
where dt='${yesterday}'
union all
select count(short_video_id)
from advert_douyin_kol_valid_vedio_daily_snapshot
where dt='${yesterday}'
union all
select count(b.short_video_id)
from (select short_video_id from advert_douyin_kol_valid_vedio_daily_snapshot where dt='${yesterday}') b
left outer join (select short_video_id from advert_douyin_kol_valid_vedio_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.short_video_id=a.short_video_id where a.short_video_id is null
union all
select count(challenge_id)
from advert_douyin_valid_kol_topic_daily_snapshot
where dt='${yesterday}'
union all
select count(b.challenge_id)
from (select challenge_id from advert_douyin_valid_kol_topic_daily_snapshot where dt='${yesterday}') b
left outer join (select challenge_id from advert_douyin_valid_kol_topic_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.challenge_id=a.challenge_id where a.challenge_id is null
union all
select count(challenge_id)
from advert_douyin_valid_topic_kol_daily_snapshot
where dt='${yesterday}'
union all
select count(b.challenge_id)
from (select challenge_id from advert_douyin_valid_topic_kol_daily_snapshot where dt='${yesterday}') b
left outer join (select challenge_id from advert_douyin_valid_topic_kol_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.challenge_id=a.challenge_id where a.challenge_id is null
union all
select count(comment_id)
from advert_douyin_valid_kol_comment_daily_snapshot
where dt='${yesterday}'
union all
select count(b.comment_id)
from (select comment_id from advert_douyin_valid_kol_comment_daily_snapshot where dt='${yesterday}') b
left outer join (select comment_id from advert_douyin_valid_kol_comment_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.comment_id=a.comment_id where a.comment_id is null
union all
select count(comment_id)
from advert_douyin_valid_comment_kol_daily_snapshot
where dt='${yesterday}'
union all
select count(b.comment_id)
from (select comment_id from advert_douyin_valid_comment_kol_daily_snapshot where dt='${yesterday}') b
left outer join (select comment_id from advert_douyin_valid_comment_kol_daily_snapshot where dt='${dayBeforeYesterday}') a
on b.comment_id=a.comment_id where a.comment_id is null
;"
echo "执行完成"
