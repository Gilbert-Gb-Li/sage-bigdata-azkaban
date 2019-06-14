#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh


date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
twentyday=`date -d "-20 day $date" +%Y-%m-%d`
echo "${date}"
echo "${yesterday}"
echo "${twentyday}"


echo "############### 抖音大屏当日最新上传视频数据 start #####################"
mysql_table_daily_create_video="tbl_douyin_big_screen_daily_create_video"

hive_sql_daily_create_video_1="select short_video_id,cast(like_count as int) as like_count,cast(share_count as int) as share_count,cast(comment_count as int) as comment_count,play_url_list,download_url_list,from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd') as video_create_time,0 as is_weighted,0 as weighted_value
from (
select * from (select *, row_number() over (partition by short_video_id order by record_time desc) as od from ias.tbl_ex_short_video_data_origin_orc where dt='${date}' and from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd')='${date}') as g where g.od=1
) as n limit 2500"
echo "${hive_sql_daily_create_video_1}"

hiveSqlToMysql "${hive_sql_daily_create_video_1}" "${date}" "${mysql_table_daily_create_video}" "short_video_id,like_count,share_count,comment_count,play_url_list,download_url_list,video_create_time,is_weighted,weighted_value" "is_weighted=0 and video_create_time"
echo "############### 抖音大屏当日最新上传视频数据 end #####################"

echo "############### 抖音大屏当日最新上传视频转发点赞评论综合值最高的视频数据 start #####################"
hive_sql_daily_create_video_2="select short_video_id,cast(like_count as int) as like_count,cast(share_count as int) as share_count,cast(comment_count as int) as comment_count,play_url_list,download_url_list,from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd') as create_time,1 as is_weighted,(cast(cast(like_count as int)*0.2 as decimal)+cast(cast(share_count as int)*0.3 as decimal)+cast(cast(comment_count as int)*0.5 as decimal))as weighted_value
from (
select * from (select *, row_number() over (partition by short_video_id order by record_time desc) as od from ias.tbl_ex_short_video_data_origin_orc where dt='${date}' and from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd')='${date}') as h where h.od=1
) as b
order by weighted_value desc limit 2500"
echo "${hive_sql_daily_create_video_2}"

hiveSqlToMysql "${hive_sql_daily_create_video_2}" "${date}" "${mysql_table_daily_create_video}" "short_video_id,like_count,share_count,comment_count,play_url_list,download_url_list,video_create_time,is_weighted,weighted_value" "is_weighted=1 and video_create_time"
echo "############### 抖音大屏当日最新上传视频转发点赞评论综合值最高的视频数据 end #####################"


echo "############### 抖音 Top 50 用户 获赞数、粉丝数、评论数 start  #####################"
mysql_table_user_rank_data="tbl_douyin_big_screen_user_rank_data"
user_top_number=50

hive_sql_user_rank_data="select a.user_id,b.nick_name,b.user_name,b.avatar_url,'0' as follower_count,b.birthday,b.sex,a.like_count,'0' as comment_count,'1' as rank_type,a.rank,'${date}' as stats_date
from
(select f.user_id,f.like_count,f.rank from (select *,row_number() over (order by cast(like_count as bigint) desc) as rank from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt='${date}' ) as f where f.rank<=${user_top_number}
) as a
left join
(select user_id,nick_name,user_name,avatar_url,birthday,sex from short_video.tbl_ex_short_video_user_daily_snapshot where dt='${date}'
) as b
on (a.user_id=b.user_id)
union all
select a.user_id,b.nick_name,b.user_name,b.avatar_url,a.follower_count,b.birthday,b.sex,'0' as like_count,'0' as comment_count,'2' as rank_type,a.rank,'${date}' as stats_date
from
(select f.user_id,f.follower_count,f.rank from (select *,row_number() over (order by cast(follower_count as bigint) desc) as rank from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt='${date}' ) as f where f.rank<=${user_top_number}
) as a
left join
(select user_id,nick_name,user_name,avatar_url,birthday,sex from short_video.tbl_ex_short_video_user_daily_snapshot where dt='${date}'
) as b
on (a.user_id=b.user_id)
union all
select a.author_id,b.nick_name,b.user_name,b.avatar_url,'0' as follower_count,b.birthday,b.sex,'0' as like_count,a.comment_count,'3' as rank_type,a.rank,'${date}' as stats_date
from
(select f.author_id,cast(f.comment_count as string) as comment_count,f.rank from(select author_id,sum(cast(comment_count as bigint)) as comment_count,row_number() over (order by sum(cast(comment_count as bigint)) desc) as rank  from short_video.tbl_ex_short_video_data_daily_snapshot where dt='${date}' group by author_id) as f where f.rank <=${user_top_number}
) as a
left join
(select user_id,nick_name,user_name,avatar_url,birthday,sex from short_video.tbl_ex_short_video_user_daily_snapshot where dt='${date}'
) as b
on (a.author_id=b.user_id)"
echo "${hive_sql_user_rank_data}"

hiveSqlToMysqlUTF8MB4 "${hive_sql_user_rank_data}" "${date}" "${mysql_table_user_rank_data}" "user_id,nick_name,user_name,avatar_url,follower_count,birthday,sex,like_count,comment_count,rank_type,serial_number,date" "date"
echo "############### 抖音 Top 50 用户 获赞数、粉丝数、评论数 end  #####################"

echo "############### 抖音 Top 50 当日挑战最多的主题 和 音乐 start  #####################"
mysql_table_daily_use_challenge_music_rank="tbl_douyin_big_screen_daily_use_challenge_music_rank"
use_challenge_music_top_number=50

hive_sql_daily_use_challenge_music_rank="select '' as music_id,'' as music_play_url,'false' as music_is_original,'' as music_author,'' as music_name,0 as music_user_count,b.challenge_id,b.challenge_name,b.challenge_user_count,1 as rank_type,b.rank_two as serial_number,'${date}' as stats_date
from(
select a.shallenge_id as challenge_id,a.challenge_name,a.challenge_count as challenge_user_count,row_number() over(order by a.challenge_count desc) as rank_two
from (
select p.shallenge_id,p.challenge_name,o.challenge_count,row_number() over(partition by p.shallenge_id order by cast(o.challenge_count as bigint) desc) as rank from
(select shallenge_id,challenge_name from short_video.tbl_ex_short_video_data_daily_snapshot where shallenge_id is not null and shallenge_id<>'' and dt='${date}' and from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd')='${date}') as p
left join
(select shallenge_id,count(shallenge_id) as challenge_count from short_video.tbl_ex_short_video_data_daily_snapshot where shallenge_id is not null and shallenge_id<>'' and dt='2018-06-18' and from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd')='2018-06-18' group by shallenge_id) as o
on (p.shallenge_id=o.shallenge_id)
) as a where a.rank=1 ) as b where b.rank_two<${use_challenge_music_top_number}
union all
select f.music_id,f.music_play_url,f.music_is_original,f.music_author,f.music_name,f.music_count as music_user_count,'' as challenge_id,'' as challenge_name,0 as challenge_user_count,2 as rank_type,f.rank_two as serial_number,'${date}' as stats_date
from(
select g.music_id,g.music_name,g.music_author,g.music_is_original,g.music_play_url,g.music_count,row_number () over(order by g.music_count desc) as rank_two
from
(select t.music_id,t.music_name,t.music_author,t.music_is_original,t.music_play_url,t.tag,y.music_count,row_number() over(partition by t.music_id order by t.tag) as rank
from
(select music_id,music_name,music_author,music_is_original,music_play_url,case when music_is_original='true' then 1 when music_is_original='false' then 2 when (music_is_original<>'false' or music_is_original<>'true') then 3 end tag
from short_video.tbl_ex_short_video_data_daily_snapshot where music_id is not null and music_id<>'' and dt='${date}' and from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd')='${date}'
) as t
left join
(select music_id,count(music_id) as music_count from short_video.tbl_ex_short_video_data_daily_snapshot where music_id is not null and music_id<>'' and dt='${date}' and from_unixtime(cast(substr(video_create_time,1,10) as bigint),'yyyy-MM-dd')='${date}' group by music_id
) as y
on (t.music_id=y.music_id)) as g where g.rank=1) as f where f.rank_two<${use_challenge_music_top_number}"
echo "${hive_sql_daily_use_challenge_music_rank}"

hiveSqlToMysqlUTF8MB4 "${hive_sql_daily_use_challenge_music_rank}" "${date}" "${mysql_table_daily_use_challenge_music_rank}" "music_id,music_play_url,music_is_original,music_author,music_name,music_user_count,challenge_id,challenge_name,challenge_user_count,rank_type,serial_number,date" "date"
echo "############### 抖音 Top 50 当日挑战最多的主题 和 音乐 end  #####################"


echo "############### 抖音 所有主题的视频总量和新增 start  #####################"
mysql_table_daily_challenge_amount="tbl_douyin_big_screen_daily_challenge_amount"

hive_sql_daily_challenge_amount="
select a.challenge_user_total,(a.challenge_user_total-b.yesterday_total) as challenge_new_user_total,'${date}' as start_data from
(select 1 as id,sum(cast(challenge_user_count as bigint)) as challenge_user_total from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt='${date}') as a
join
(select 1 as id,sum(cast(challenge_user_count as bigint)) as yesterday_total from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt='${yesterday}') as b
on (a.id=b.id)"
echo "${hive_sql_daily_challenge_amount}"

hiveSqlToMysql "${hive_sql_daily_challenge_amount}" "${date}" "${mysql_table_daily_challenge_amount}" "challenge_user_total,challenge_new_user_total,date" "date"
echo "############### 抖音 所有主题的视频总量和新增 end  #####################"


echo "############### 抖音 Top 13 挑战主题的视频总量和新增以及对应的视频url start  #####################"
mysql_table_daily_top_challenge_video_data="tbl_douyin_big_screen_daily_top_challenge_video_data"
top_challenge_video_number=10

hive_sql_daily_top_challenge_video_data="select b.challenge_id,b.challenge_name,b.challenge_user_count,b.challenge_description,b.challenge_new_user_count,c.short_video_id,c.play_url_list,c.download_url_list,'${date}' as start_data
from
(select a.challenge_id,a.challenge_name,a.challenge_user_count,a.challenge_description,a.challenge_new_user_count,a.rank from (select challenge_id,challenge_name,challenge_user_count,challenge_description,challenge_new_user_count,row_number() over(order by cast(challenge_user_count as bigint) desc) as rank from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt='${date}') as a where a.rank<=13
) as b
left join
(select d.shallenge_id,d.short_video_id,d.play_url_list,d.download_url_list from (select shallenge_id,short_video_id,play_url_list,download_url_list,row_number() over(partition by shallenge_id order by cast(CEILING(rand()* 1000000) as bigint) desc) as rank_two from short_video.tbl_ex_short_video_data_daily_snapshot where dt='${date}') as d where d.rank_two<=${top_challenge_video_number}
) as c
on (b.challenge_id=c.shallenge_id)"
echo "${hive_sql_daily_top_challenge_video_data}"

hiveSqlToMysqlUTF8MB4 "${hive_sql_daily_top_challenge_video_data}" "${date}" "${mysql_table_daily_top_challenge_video_data}" "challenge_id,challenge_name,challenge_user_count,challenge_description,challenge_new_user_count,short_video_id,play_url_list,download_url_list,date" "date"
echo "############### 抖音 Top 13 挑战主题的视频总量和新增以及对应的视频url end  #####################"


echo "############### 抖音 分城市视频数量和比例排行 start  #####################"
mysql_table_user_location_video_data="tbl_douyin_big_screen_user_location_video_data"

hive_sql_user_location_video_data="select a.user_id,b.nick_name,b.user_name,b.user_location,a.short_video_count,'${date}' as start_date from
(select user_id,short_video_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt='${date}' and cast(short_video_count as bigint)>0
) as a
left join
(select user_id,nick_name,user_name,user_location  from short_video.tbl_ex_short_video_user_daily_snapshot where dt='${date}'
) as b
on (a.user_id=b.user_id) where b.user_location<>'' and b.user_location is not null"
echo "${hive_sql_user_location_video_data}"

hiveSqlToMysqlUTF8MB4 "${hive_sql_user_location_video_data}" "${date}" "${mysql_table_user_location_video_data}" "user_id,nick_name,user_name,user_location,short_video_count,date" "date"
echo "############### 抖音 分城市视频数量和比例排行 end  #####################"
#分城市视频数量和比例排行 在插曲数库有些问题，还没有解决。