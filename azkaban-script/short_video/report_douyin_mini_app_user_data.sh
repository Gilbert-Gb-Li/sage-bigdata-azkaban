#!/bin/sh
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
#六
day8=`date -d "-7 day ${yesterday}" +%Y-%m-%d`
#日
day14=`date -d "-13 day ${yesterday}" +%Y-%m-%d`
#六
day15=`date -d "-14 day ${yesterday}" +%Y-%m-%d`
#日
day20=`date -d "-19 day ${yesterday}" +%Y-%m-%d`

mysql_table1="tbl_douyin_mini_app_user_like_rank_data"
mysql_table2="tbl_douyin_mini_app_user_follower_rank_data"
mysql_table3="tbl_douyin_mini_app_user_comment_rank_data"
mysql_table4="tbl_douyin_mini_app_user_soar_rank_data"
mysql_table5="tbl_douyin_mini_app_user_detail_data"
tmp_mysql_table1="tmp_douyin_mini_app_hot_video_data"
tmp_mysql_table2="tmp_douyin_mini_app_week_chart_data"

#创建hive临时表用来存放播主榜单，目的是为了方便后面的任务，不用重复跑相同的程序。节省资源。待程序执行完成后将会自动删除
currentTime=$(date "+%s%N")
tmpTable=tmp.douyin_user_${currentTime}
executeHiveCommand "create table "${tmpTable}" (stat_date string,user_id string,nick_name string,avatar_url string,rank_value string,rank_type string,list_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';"

echo "========================点赞排行榜========================="

hive_sql1="insert into table ${tmpTable}
select '${yesterday}' as stat_date,a.user_id,a.nick_name,a.avatar_url,b.like_count,'0' as rank_type,'' as list_type from
(select user_id,nick_name,avatar_url from short_video.tbl_ex_short_video_user_daily_snapshot where dt = '${yesterday}') a
join
(select user_id,like_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${yesterday}' order by cast(like_count as bigint) desc limit 100) b
on a.user_id = b.user_id"

executeHiveCommand "${hive_sql1}"

hive_to_mysql1="select stat_date,user_id,nick_name,avatar_url,rank_value from ${tmpTable} where rank_type = '0'"

hiveSqlToMysql "${hive_to_mysql1}" "${yesterday}" "${mysql_table1}" "stat_date,user_id,nick_name,avatar_url,like_count" "stat_date"

echo "========================粉丝排行榜========================="

hive_sql2="insert into table ${tmpTable}
select '${yesterday}' as stat_date,a.user_id,a.nick_name,a.avatar_url,b.follower_count,'1' as rank_type,'' as list_type from
(select user_id,nick_name,avatar_url from short_video.tbl_ex_short_video_user_daily_snapshot where dt = '${yesterday}') a
join
(select user_id,follower_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${yesterday}' order by cast(follower_count as bigint) desc limit 100) b
on a.user_id = b.user_id"

executeHiveCommand "${hive_sql2}"

hive_to_mysql2="select stat_date,user_id,nick_name,avatar_url,rank_value from ${tmpTable} where rank_type = '1'"

hiveSqlToMysql "${hive_to_mysql2}" "${yesterday}" "${mysql_table2}" "stat_date,user_id,nick_name,avatar_url,follower_count" "stat_date"

echo "========================评论排行榜========================="

hive_sql3="insert into table ${tmpTable}
select '${yesterday}' as stat_date,a.user_id,a.nick_name,a.avatar_url,b.comment_count,'2' as rank_type,'' as list_type from
(select user_id,nick_name,avatar_url from short_video.tbl_ex_short_video_user_daily_snapshot where dt = '${yesterday}') a
join
(select author_id,comment_count from
(select author_id,sum(cast(comment_count as bigint)) as comment_count
from short_video.tbl_ex_short_video_data_daily_snapshot
where dt = '${yesterday}' group by author_id
) t order by comment_count desc limit 100) b
on a.user_id = b.author_id"

executeHiveCommand "${hive_sql3}"

hive_to_mysql3="select stat_date,user_id,nick_name,avatar_url,rank_value from ${tmpTable} where rank_type = '2'"

hiveSqlToMysql "${hive_to_mysql3}" "${yesterday}" "${mysql_table3}" "stat_date,user_id,nick_name,avatar_url,comment_count" "stat_date"

echo "========================飙升日排行榜========================="

hive_sql4="insert into table ${tmpTable}
select '${yesterday}' as stat_date,a.user_id,a.nick_name,a.avatar_url,b.Soar_num,'3' as rank_type,'1' as list_type from
(select user_id,nick_name,avatar_url from short_video.tbl_ex_short_video_user_daily_snapshot where dt = '${yesterday}') a
join
(select user_id,soar_num from
(select user_id,(like_increase*0.1+share_increase*0.3+follower_increase*0.3+comment_increase*0.3) as soar_num from
(select a.user_id,
case when b.like_increase is null or b.like_increase = 0 then 0 else (a.like_increase/b.like_increase)*100 end like_increase,
case when b.follower_increase is null or b.follower_increase = 0 then 0 else (a.follower_increase/b.follower_increase)*100 end follower_increase,
case when b.comment_increase is null  or b.comment_increase = 0 then 0 else (a.comment_increase/b.comment_increase)*100 end comment_increase,
case when b.share_increase is null or b.share_increase = 0 then 0 else (a.share_increase/b.share_increase)*100 end share_increase from
(select t1.user_id,
case when t2.like_count is null then t1.like_count else t1.like_count-t2.like_count end like_increase,
case when t2.follower_count is null then t1.follower_count else t1.follower_count-t2.follower_count end follower_increase,
case when t2.comment_count is null then t1.comment_count else t1.comment_count-t2.comment_count end comment_increase,
case when t2.share_count is null then t1.share_count else t1.share_count-t2.share_count end share_increase from
(select a.user_id,
cast(a.like_count as bigint) as like_count,
cast(a.follower_count as bigint) as follower_count,
case when b.comment_count is null then 0 else cast(b.comment_count as bigint) end comment_count,
case when b.share_count is null then 0 else cast(b.share_count as bigint) end share_count from
(select user_id,like_count,follower_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${yesterday}' and like_count != '' and follower_count != '') a
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count,sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${yesterday}' and comment_count != '' and share_count != '' group by author_id) b
on a.user_id = b.author_id) t1
left join
(select a.user_id,
cast(a.like_count as bigint) as like_count,
cast(a.follower_count as bigint) as follower_count,
case when b.comment_count is null then 0 else cast(b.comment_count as bigint) end comment_count,
case when b.share_count is null then 0 else cast(b.share_count as bigint) end share_count from
(select user_id,like_count,follower_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day2}' and like_count != '' and follower_count != '') a
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count,sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day2}' and comment_count != '' and share_count != '' group by author_id) b
on a.user_id = b.author_id) t2
on t1.user_id = t2.user_id) a
left join
(select t1.user_id,
case when t2.like_count is null then t1.like_count else t1.like_count-t2.like_count end like_increase,
case when t2.follower_count is null then t1.follower_count else t1.follower_count-t2.follower_count end follower_increase,
case when t2.comment_count is null then t1.comment_count else t1.comment_count-t2.comment_count end comment_increase,
case when t2.share_count is null then t1.share_count else t1.share_count-t2.share_count end share_increase from
(select a.user_id,
cast(a.like_count as bigint) as like_count,
cast(a.follower_count as bigint) as follower_count,
case when b.comment_count is null then 0 else cast(b.comment_count as bigint) end comment_count,
case when b.share_count is null then 0 else cast(b.share_count as bigint) end share_count from
(select user_id,like_count,follower_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day2}' and like_count != '' and follower_count != '') a
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count,sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day2}' and comment_count != '' and share_count != '' group by author_id) b
on a.user_id = b.author_id) t1
left join
(select a.user_id,
cast(a.like_count as bigint) as like_count,
cast(a.follower_count as bigint) as follower_count,
case when b.comment_count is null then 0 else cast(b.comment_count as bigint) end comment_count,
case when b.share_count is null then 0 else cast(b.share_count as bigint) end share_count from
(select user_id,like_count,follower_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day3}' and like_count != '' and follower_count != '') a
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count,sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day3}' and comment_count != '' and share_count != '' group by author_id) b
on a.user_id = b.author_id) t2
on t1.user_id = t2.user_id) b
on a.user_id = b.user_id) t3) t4
order by soar_num desc limit 100) b
on a.user_id = b.user_id"

executeHiveCommand "${hive_sql4}"

hive_to_mysql4="select stat_date,user_id,nick_name,avatar_url,rank_value,list_type from ${tmpTable} where rank_type = '3' and list_type = '1'"

hiveSqlToMysqlNoDelete "${hive_to_mysql4}" "${mysql_table4}" "stat_date,user_id,nick_name,avatar_url,soar_value,list_type"

echo "========================飙升周排行榜========================="

a=`date -d "${today}" +%w`
echo $a
b=$(($a+1))
echo $b
st_date1=`date -d "$b day ago ${today}" +%Y-%m-%d`
echo $st_date1
st_date2=`date -d "7 day ago ${st_date1}" +%Y-%m-%d`
echo $st_date2
st_date3=`date -d "7 day ago ${st_date2}" +%Y-%m-%d`
echo $st_date3

hive_sql5="insert into table ${tmpTable}
select '${yesterday}' as stat_date,a.user_id,a.nick_name,a.avatar_url,b.week_soar_num,'3' as rank_type,'2' as list_type from
(select user_id,nick_name,avatar_url from short_video.tbl_ex_short_video_user_daily_snapshot where dt = '${yesterday}') a
join
(select user_id,week_soar_num from
(select user_id,(week_like_increase*0.1+week_share_increase*0.3+week_follower_increase*0.3+week_comment_increase*0.3) as week_soar_num from
(select w1.user_id,
case when w2.week_like_increase is null or w2.week_like_increase = 0 then 0 else (w1.week_like_increase/w2.week_like_increase)*100 end week_like_increase,
case when w2.week_follower_increase is null or w2.week_follower_increase = 0 then 0 else (w1.week_follower_increase/w2.week_follower_increase)*100 end week_follower_increase,
case when w2.week_comment_increase is null or w2.week_comment_increase = 0 then 0 else (w1.week_comment_increase/w2.week_comment_increase)*100 end week_comment_increase,
case when w2.week_share_increase is null or w2.week_share_increase = 0 then 0 else (w1.week_share_increase/w2.week_share_increase)*100 end week_share_increase from
(select t1.user_id,
case when t2.like_count is null then t1.like_count else t1.like_count-t2.like_count end week_like_increase,
case when t2.follower_count is null then t1.follower_count else t1.follower_count-t2.follower_count end week_follower_increase,
case when t2.comment_count is null then t1.comment_count else t1.comment_count-t2.comment_count end week_comment_increase,
case when t2.share_count is null then t1.share_count else t1.share_count-t2.share_count end week_share_increase from
(select a.user_id,a.like_count,a.follower_count,
case when b.comment_count is null then 0 else b.comment_count end comment_count,
case when b.share_count is null then 0 else b.share_count end share_count from
(select user_id,cast(like_count as bigint) as like_count,cast(follower_count as bigint) as follower_count
from short_video.tbl_ex_short_video_user_detail_daily_snapshot
where dt = '${st_date1}' and like_count != '' and follower_count != '') a
left join
(select author_id,
sum(cast(comment_count as bigint)) as comment_count,
sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot
where dt = '${st_date1}' and comment_count != '' and share_count != ''
group by author_id) b
on a.user_id = b.author_id) t1
left join
(select a.user_id,a.like_count,a.follower_count,
case when b.comment_count is null then 0 else b.comment_count end comment_count,
case when b.share_count is null then 0 else b.share_count end share_count from
(select user_id,cast(like_count as bigint) as like_count,cast(follower_count as bigint) as follower_count
from short_video.tbl_ex_short_video_user_detail_daily_snapshot
where dt = '${st_date2}' and like_count != '' and follower_count != '') a
left join
(select author_id,
sum(cast(comment_count as bigint)) as comment_count,
sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot
where dt = '${st_date2}' and comment_count != '' and share_count != ''
group by author_id) b
on a.user_id = b.author_id) t2
on t1.user_id = t2.user_id) w1
left join
(select t1.user_id,
case when t2.like_count is null then t1.like_count else t1.like_count-t2.like_count end week_like_increase,
case when t2.follower_count is null then t1.follower_count else t1.follower_count-t2.follower_count end week_follower_increase,
case when t2.comment_count is null then t1.comment_count else t1.comment_count-t2.comment_count end week_comment_increase,
case when t2.share_count is null then t1.share_count else t1.share_count-t2.share_count end week_share_increase from
(select a.user_id,a.like_count,a.follower_count,
case when b.comment_count is null then 0 else b.comment_count end comment_count,
case when b.share_count is null then 0 else b.share_count end share_count from
(select user_id,cast(like_count as bigint) as like_count,cast(follower_count as bigint) as follower_count
from short_video.tbl_ex_short_video_user_detail_daily_snapshot
where dt = '${st_date2}' and like_count != '' and follower_count != '') a
left join
(select author_id,
sum(cast(comment_count as bigint)) as comment_count,
sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot
where dt = '${st_date2}' and comment_count != '' and share_count != ''
group by author_id) b
on a.user_id = b.author_id) t1
left join
(select a.user_id,a.like_count,a.follower_count,
case when b.comment_count is null then 0 else b.comment_count end comment_count,
case when b.share_count is null then 0 else b.share_count end share_count from
(select user_id,cast(like_count as bigint) as like_count,cast(follower_count as bigint) as follower_count
from short_video.tbl_ex_short_video_user_detail_daily_snapshot
where dt = '${st_date3}' and like_count != '' and follower_count != '') a
left join
(select author_id,
sum(cast(comment_count as bigint)) as comment_count,
sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot
where dt = '${st_date3}' and comment_count != '' and share_count != ''
group by author_id) b
on a.user_id = b.author_id) t2
on t1.user_id = t2.user_id) w2
on w1.user_id = w2.user_id) t) s
order by week_soar_num desc limit 100) b
on a.user_id = b.user_id"

executeHiveCommand "${hive_sql5}"
hive_to_mysql5="select stat_date,user_id,nick_name,avatar_url,rank_value,list_type from ${tmpTable} where rank_type = '3' and list_type = '2'"
hiveSqlToMysqlNoDelete "${hive_to_mysql5}" "${mysql_table4}" "stat_date,user_id,nick_name,avatar_url,soar_value,list_type"

echo "========================播主详情页========================="

hive_sql6="select '${yesterday}' as stat_date,v1.user_id,v2.nick_name,v2.avatar_url,v2.signature,v2.birthday,v2.user_location,v2.constellation,v2.sex,v1.like_count,v1.follower_count,v1.comment_count,v1.share_count from
(select t1.user_id,t1.like_count,t1.follower_count,t2.comment_count,t2.share_count from
(select a.user_id,a.like_count,a.follower_count from
(select user_id,like_count,follower_count from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${yesterday}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count,sum(cast(share_count as bigint)) as share_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${yesterday}' group by author_id) t2
on t1.user_id = t2.author_id) v1
left join
(select user_id,nick_name,avatar_url,signature,birthday,user_location,constellation,sex from short_video.tbl_ex_short_video_user_daily_snapshot where dt = '${yesterday}') v2
on v1.user_id = v2.user_id"

hiveSqlToMysql "${hive_sql6}" "${yesterday}" "${mysql_table5}" "stat_date,user_id,nick_name,avatar_url,signature,birthday,user_location,constellation,sex,like_count,follower_count,comment_count,share_count" "stat_date"

echo "========================播主详情页最热视频模块========================="

hive_sql7="select '${yesterday}' as stat_date,author_id,concat_ws('\;',collect_set(hot_video)) from
(select r.author_id,concat_ws('\;',concat('[\"',concat_ws('\",\"',r.cover_url_list),'\"]'),concat('[\"',concat_ws('\",\"',r.play_url_list),'\"]'),concat('[',cast(r.like_count as string),']')) as hot_video from
(select t.*,row_number() over (partition by author_id order by like_count desc) num from
(select a.author_id,cast(a.like_count as bigint) as like_count,a.cover_url_list,a.play_url_list from
(select author_id,like_count,cover_url_list,play_url_list from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${yesterday}') a
join
(select distinct user_id from ${tmpTable}) b
on a.author_id = b.user_id) t) r
where r.num <= 6) s
group by author_id"

create_tmp_table1_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_table1} (stat_date varchar(255),user_id VARCHAR(50),hot_video TEXT)"
execSqlOnMysql "${create_tmp_table1_sql}"
hiveSqlToMysqlNoDelete "${hive_sql7}" "${tmp_mysql_table1}" "stat_date,user_id,hot_video"

update_hot_video_sql="UPDATE $mysql_table5 a
JOIN (
  SELECT * FROM $tmp_mysql_table1
  WHERE stat_date='${yesterday}'
) b
ON a.stat_date=b.stat_date AND a.user_id=b.user_id
SET a.hot_video=b.hot_video
WHERE a.stat_date='${yesterday}';"

execSqlOnMysql "${update_hot_video_sql}"

drop_hot_video_sql="DROP TABLE ${tmp_mysql_table1}"
execSqlOnMysql "${drop_hot_video_sql}"

echo "========================播主详情页图模块========================="

hive_sql8="select '${yesterday}' as stat_date,user_id,concat_ws('\;',concat('[\"',concat_ws('\",\"',day7_like_increase,day6_like_increase,day5_like_increase,day4_like_increase,day3_like_increase,day2_like_increase,day1_like_increase),'\"]'),
concat('[\"',concat_ws('\",\"',day7_comment_increase,day6_comment_increase,day5_comment_increase,day4_comment_increase,day3_comment_increase,day2_comment_increase,day1_comment_increase),'\"]')) from
(select user_id,
cast(day1_like_count-day2_like_count as string) as day1_like_increase,
cast(day2_like_count-day3_like_count as string) as day2_like_increase,
cast(day3_like_count-day4_like_count as string) as day3_like_increase,
cast(day4_like_count-day5_like_count as string) as day4_like_increase,
cast(day5_like_count-day6_like_count as string) as day5_like_increase,
cast(day6_like_count-day7_like_count as string) as day6_like_increase,
cast(day7_like_count-day8_like_count as string) as day7_like_increase,
cast(day1_comment_count-day2_comment_count as string) as day1_comment_increase,
cast(day2_comment_count-day3_comment_count as string) as day2_comment_increase,
cast(day3_comment_count-day4_comment_count as string) as day3_comment_increase,
cast(day4_comment_count-day5_comment_count as string) as day4_comment_increase,
cast(day5_comment_count-day6_comment_count as string) as day5_comment_increase,
cast(day6_comment_count-day7_comment_count as string) as day6_comment_increase,
cast(day7_comment_count-day8_comment_count as string) as day7_comment_increase from
(select s6.user_id,s6.day1_like_count,s6.day1_comment_count,s6.day2_like_count,s6.day2_comment_count,s6.day3_like_count,s6.day3_comment_count,s6.day4_like_count,s6.day4_comment_count,s6.day5_like_count,s6.day5_comment_count,s6.day6_like_count,s6.day6_comment_count,s6.day7_like_count,s6.day7_comment_count,
case when w8.day8_like_count is null then 0 else w8.day8_like_count end day8_like_count,
case when w8.day8_comment_count is null then 0 else w8.day8_comment_count end day8_comment_count from
(select s5.user_id,s5.day1_like_count,s5.day1_comment_count,s5.day2_like_count,s5.day2_comment_count,s5.day3_like_count,s5.day3_comment_count,s5.day4_like_count,s5.day4_comment_count,s5.day5_like_count,s5.day5_comment_count,s5.day6_like_count,s5.day6_comment_count,
case when w7.day7_like_count is null then 0 else w7.day7_like_count end day7_like_count,
case when w7.day7_comment_count is null then 0 else w7.day7_comment_count end day7_comment_count from
(select s4.user_id,s4.day1_like_count,s4.day1_comment_count,s4.day2_like_count,s4.day2_comment_count,s4.day3_like_count,s4.day3_comment_count,s4.day4_like_count,s4.day4_comment_count,s4.day5_like_count,s4.day5_comment_count,
case when w6.day6_like_count is null then 0 else w6.day6_like_count end day6_like_count,
case when w6.day6_comment_count is null then 0 else w6.day6_comment_count end day6_comment_count from
(select s3.user_id,s3.day1_like_count,s3.day1_comment_count,s3.day2_like_count,s3.day2_comment_count,s3.day3_like_count,s3.day3_comment_count,s3.day4_like_count,s3.day4_comment_count,
case when w5.day5_like_count is null then 0 else w5.day5_like_count end day5_like_count,
case when w5.day5_comment_count is null then 0 else w5.day5_comment_count end day5_comment_count from
(select s2.user_id,s2.day1_like_count,s2.day1_comment_count,s2.day2_like_count,s2.day2_comment_count,s2.day3_like_count,s2.day3_comment_count,
case when w4.day4_like_count is null then 0 else w4.day4_like_count end day4_like_count,
case when w4.day4_comment_count is null then 0 else w4.day4_comment_count end day4_comment_count from
(select s1.user_id,s1.day1_like_count,s1.day1_comment_count,s1.day2_like_count,s1.day2_comment_count,
case when w3.day3_like_count is null then 0 else w3.day3_like_count end day3_like_count,
case when w3.day3_comment_count is null then 0 else w3.day3_comment_count end day3_comment_count from
(select w1.user_id,w1.like_count as day1_like_count,w1.comment_count as day1_comment_count,
case when w2.like_count is null then 0 else w2.like_count end day2_like_count,
case when w2.comment_count is null then 0 else w2.comment_count end day2_comment_count from
(select t1.user_id,t1.like_count,
case when t2.comment_count is null then 0 else t2.comment_count end comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${yesterday}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${yesterday}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w1
left join
(select t1.user_id,t1.like_count,
case when t2.comment_count is null then 0 else t2.comment_count end comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day2}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day2}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w2
on w1.user_id = w2.user_id) s1
left join
(select t1.user_id,t1.like_count as day3_like_count,
case when t2.comment_count is null then 0 else t2.comment_count end day3_comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day3}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day3}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w3
on s1.user_id = w3.user_id) s2
left join
(select t1.user_id,t1.like_count as day4_like_count,
case when t2.comment_count is null then 0 else t2.comment_count end day4_comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day4}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day4}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w4
on s2.user_id = w4.user_id) s3
left join
(select t1.user_id,t1.like_count as day5_like_count,
case when t2.comment_count is null then 0 else t2.comment_count end day5_comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day5}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day5}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w5
on s3.user_id = w5.user_id) s4
left join
(select t1.user_id,t1.like_count as day6_like_count,
case when t2.comment_count is null then 0 else t2.comment_count end day6_comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day6}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day6}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w6
on s4.user_id = w6.user_id) s5
left join
(select t1.user_id,t1.like_count as day7_like_count,
case when t2.comment_count is null then 0 else t2.comment_count end day7_comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day7}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day7}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w7
on s5.user_id = w7.user_id) s6
left join
(select t1.user_id,t1.like_count as day8_like_count,
case when t2.comment_count is null then 0 else t2.comment_count end day8_comment_count from
(select a.user_id,a.like_count from
(select user_id,cast(like_count as bigint) from short_video.tbl_ex_short_video_user_detail_daily_snapshot where dt = '${day8}') a
join
(select distinct user_id from ${tmpTable}) b
on a.user_id = b.user_id) t1
left join
(select author_id,sum(cast(comment_count as bigint)) as comment_count from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${day8}' and comment_count != '' and share_count != '' group by author_id) t2
on t1.user_id = t2.author_id) w8
on s6.user_id = w8.user_id) t) r"

create_tmp_table2_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_table2} (stat_date varchar(255),user_id VARCHAR(50),week_chart VARCHAR(255))"
execSqlOnMysql "${create_tmp_table2_sql}"
hiveSqlToMysqlNoDelete "${hive_sql8}" "${tmp_mysql_table2}" "stat_date,user_id,week_chart"

update_week_chart_sql="UPDATE $mysql_table5 a
JOIN (
  SELECT * FROM $tmp_mysql_table2
  WHERE stat_date='${yesterday}'
) b
ON a.stat_date=b.stat_date AND a.user_id=b.user_id
SET a.week_chart=b.week_chart
WHERE a.stat_date='${yesterday}';"

execSqlOnMysql "${update_week_chart_sql}"

drop_week_chart_sql="DROP TABLE ${tmp_mysql_table2}"
execSqlOnMysql "${drop_week_chart_sql}"

executeHiveCommand "drop table ${tmpTable}"
