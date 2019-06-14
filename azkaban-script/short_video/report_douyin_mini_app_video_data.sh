#！/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

#六
yesterday=$1
#日
today=`date -d "+1 day ${yesterday}" +%Y-%m-%d`
day2=`date -d "-1 day ${yesterday}" +%Y-%m-%d`
day3=`date -d "-2 day ${yesterday}" +%Y-%m-%d`
day4=`date -d "-3 day ${yesterday}" +%Y-%m-%d`
day5=`date -d "-4 day ${yesterday}" +%Y-%m-%d`
day6=`date -d "-5 day ${yesterday}" +%Y-%m-%d`
#日
day7=`date -d "-6 day ${yesterday}" +%Y-%m-%d`

mysql_table1="tbl_douyin_mini_app_video_rank_data"

echo "=========================视频点赞排行榜============================"

hive_sql1="select '${yesterday}' as stat_date,short_video_id,cover_url_list,play_url_list,like_count,1 as rank_type from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '2018-06-22' order by cast(like_count as bigint) desc limit 100"

hiveSqlToMysqlNoDelete "${hive_sql1}" "${mysql_table1}" "stat_date,short_video_id,cover_url_list,play_url_list,rank_value,rank_type"

echo "=========================视频评论排行榜============================"

hive_sql2="select '${yesterday}' as stat_date,short_video_id,cover_url_list,play_url_list,comment_count,2 as rank_type from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '2018-06-22' order by cast(comment_count as bigint) desc limit 100"

hiveSqlToMysqlNoDelete "${hive_sql2}" "${mysql_table1}" "stat_date,short_video_id,cover_url_list,play_url_list,rank_value,rank_type"

echo "=========================视频飙升排行榜============================"

a=`date -d "${today}" +%w`
echo $a
b=$(($a+1))
echo $b
st_date1=`date -d "$b day ago ${today}" +%Y-%m-%d`
echo $st_date1
st_date2=`date -d "7 day ago ${st_date1}" +%Y-%m-%d`
echo $st_date2
st_date3=`date -d "7 day ago ${st_date2}" +%Y-%m-%d`

hive_sql3="select '${yesterday}' as stat_date,t.short_video_id,t.cover_url_list,t.play_url_list,round(a.soar_value,1),3 as rank_type from
(select short_video_id,cover_url_list,play_url_list from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${yesterday}') t
join
(select short_video_id,soar_value from
(select short_video_id,week_like_increase*0.7+week_comment_increase*0.3 as soar_value from
(select t1.short_video_id,
case when t2.week_like_increase is null or t2.week_like_increase = 0 then 0 else (t1.week_like_increase/t2.week_like_increase)*100 end week_like_increase,
case when t2.week_comment_increase is null or t2.week_comment_increase = 0 then 0 else (t1.week_comment_increase/t2.week_comment_increase)* 100 end week_comment_increase from
(select a.short_video_id,
case when b.like_count is null then a.like_count else a.like_count-b.like_count end week_like_increase,
case when b.comment_count is null then a.comment_count else a.comment_count-b.comment_count end week_comment_increase from
(select short_video_id,cast(like_count as bigint),cast(comment_count as bigint) from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${st_date1}') a
left join
(select short_video_id,cast(like_count as bigint),cast(comment_count as bigint) from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${st_date2}') b
on a.short_video_id = b.short_video_id) t1
left join
(select a.short_video_id,
case when b.like_count is null then a.like_count else a.like_count-b.like_count end week_like_increase,
case when b.comment_count is null then a.comment_count else a.comment_count-b.comment_count end week_comment_increase from
(select short_video_id,cast(like_count as bigint),cast(comment_count as bigint) from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${st_date2}') a
left join
(select short_video_id,cast(like_count as bigint),cast(comment_count as bigint) from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${st_date3}') b
on a.short_video_id = b.short_video_id) t2
on t1.short_video_id = t2.short_video_id) t) r
order by soar_value desc limit 100) a
on t.short_video_id = a.short_video_id"

hiveSqlToMysqlNoDelete "${hive_sql3}" "${mysql_table1}" "stat_date,short_video_id,cover_url_list,play_url_list,rank_value,rank_type"
