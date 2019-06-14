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
day8=`date -d "-7 day ${yesterday}" +%Y-%m-%d`

mysql_table1="tbl_douyin_mini_app_challenge_rank_data"
mysql_table2="tbl_douyin_mini_app_challenge_detail_data"
tmp_mysql_table1="tmp_douyin_mini_app_challenge_hot_video_data"
tmp_mysql_table2="tmp_douyin_mini_app_challenge_week_chart_data"

#创建hive临时表用来存放话题榜单，目的是为了方便后面的任务，不用重复跑相同的程序。节省资源。待程序执行完成后将会自动删除
currentTime=$(date "+%s%N")
tmpTable=tmp.douyin_challenge_${currentTime}
executeHiveCommand "create table "${tmpTable}" (stat_date string,challenge_id string,challenge_name string,rank_value string,rank_type string) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';"

echo "=====================最热话题排行榜========================"

hive_sql1="insert into table ${tmpTable}
select '${yesterday}' as stat_date,challenge_id,challenge_name,challenge_user_count,'1' as rank_type
from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${yesterday}'
order by cast(challenge_user_count as bigint) desc limit 100"

executeHiveCommand "${hive_sql1}"

echo "=====================飙升话题排行榜========================"

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

hive_sql2="insert into table ${tmpTable}
select '${yesterday}' as stat_date,challenge_id,challenge_name,soar_value,'2' as rank_type from
(select t1.challenge_id,t1.challenge_name,
case when t2.week_challenge_increase is null or t2.week_challenge_increase = 0 then 0 else round((t1.week_challenge_increase/t2.week_challenge_increase)*100,1) end soar_value
from (select a.challenge_id,a.challenge_name,
case when b.challenge_user_count is null then a.challenge_user_count else a.challenge_user_count-b.challenge_user_count end week_challenge_increase from
(select challenge_id,challenge_name,cast(challenge_user_count as bigint)
from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${st_date1}') a
left join
(select challenge_id,challenge_name,cast(challenge_user_count as bigint)
from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${st_date2}') b
on a.challenge_id = b.challenge_id) t1
left join
(select a.challenge_id,a.challenge_name,
case when b.challenge_user_count is null then a.challenge_user_count else a.challenge_user_count-b.challenge_user_count end week_challenge_increase from
(select challenge_id,challenge_name,cast(challenge_user_count as bigint)
from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${st_date2}') a
left join
(select challenge_id,challenge_name,cast(challenge_user_count as bigint)
from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${st_date3}') b
on a.challenge_id = b.challenge_id) t2
on t1.challenge_id = t2.challenge_id) t
order by soar_value desc limit 100"

executeHiveCommand "${hive_sql2}"

hive_to_mysql2="select stat_date,challenge_id,challenge_name,rank_value,rank_type from ${tmpTable}"
hiveSqlToMysqlNoDelete "${hive_to_mysql2}" "${mysql_table1}" "stat_date,challenge_id,challenge_name,rank_value,rank_type"

echo "=====================话题详情页========================"

hive_sql3="select '${yesterday}' as stat_date,a.challenge_id,a.challenge_name,a.challenge_user_count from
(select challenge_id,challenge_name,challenge_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${yesterday}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id"

hiveSqlToMysqlNoDelete "${hive_sql3}" "${mysql_table2}" "stat_date,challenge_id,challenge_name,video_count"

echo "=======================话题详情页的最热视频模块=========================="
hive_sql4="select '${yesterday}' as stat_date,shallenge_id,concat_ws('\;',collect_set(hot_video)) from
(select shallenge_id,concat_ws('\;',concat('[\"',concat_ws('\",\"',a.cover_url_list),'\"]'),concat('[\"',concat_ws('\",\"',a.play_url_list),'\"]'),concat('[',cast(a.like_count as string),']')) as hot_video from
(select shallenge_id,cover_url_list,play_url_list,like_count from
(select t.*,row_number() over (partition by shallenge_id order by like_count desc) num from
(select m1.like_count,m1.cover_url_list,m1.play_url_list,m1.shallenge_id from
(select cast(like_count as bigint),cover_url_list,play_url_list,shallenge_id from short_video.tbl_ex_short_video_data_daily_snapshot where dt = '${yesterday}') m1
join
(select distinct challenge_id from ${tmpTable}) m2
on m1.shallenge_id = m2.challenge_id) t) r
where r.num <= 6) a) b
group by shallenge_id"

create_tmp_table1_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_table1} (stat_date varchar(255),challenge_id VARCHAR(50),hot_video TEXT)"
execSqlOnMysql "${create_tmp_table1_sql}"
hiveSqlToMysqlNoDelete "${hive_sql4}" "${tmp_mysql_table1}" "stat_date,challenge_id,hot_video"

update_hot_video_sql="UPDATE $mysql_table2 a
JOIN (
  SELECT * FROM $tmp_mysql_table1
  WHERE stat_date='${yesterday}'
) b
ON a.stat_date=b.stat_date AND a.challenge_id=b.challenge_id
SET a.hot_video=b.hot_video
WHERE a.stat_date='${yesterday}';"

execSqlOnMysql "${update_hot_video_sql}"

drop_hot_video_sql="DROP TABLE ${tmp_mysql_table1}"
execSqlOnMysql "${drop_hot_video_sql}"

echo "===========================话题详情页的图模块============================"

hive_sql5="select '${yesterday}' as stat_date,challenge_id,concat('[\"',concat_ws('\",\"',day6_challenge_new_user_count,day5_challenge_new_user_count,day4_challenge_new_user_count,day3_challenge_new_user_count,day2_challenge_new_user_count,day1_challenge_new_user_count,challenge_new_user_count),'\"]') from
(select w5.challenge_id,w5.challenge_new_user_count,w5.day1_challenge_new_user_count,w5.day2_challenge_new_user_count,w5.day3_challenge_new_user_count,w5.day4_challenge_new_user_count,w5.day5_challenge_new_user_count,
case when d7.day6_challenge_new_user_count is null then 0 else d7.day6_challenge_new_user_count end day6_challenge_new_user_count from
(select w4.challenge_id,w4.challenge_new_user_count,w4.day1_challenge_new_user_count,w4.day2_challenge_new_user_count,w4.day3_challenge_new_user_count,w4.day4_challenge_new_user_count,
case when d6.day5_challenge_new_user_count is null then 0 else d6.day5_challenge_new_user_count end day5_challenge_new_user_count from
(select w3.challenge_id,w3.challenge_new_user_count,w3.day1_challenge_new_user_count,w3.day2_challenge_new_user_count,w3.day3_challenge_new_user_count,
case when d5.day4_challenge_new_user_count is null then 0 else d5.day4_challenge_new_user_count end day4_challenge_new_user_count from
(select w2.challenge_id,w2.challenge_new_user_count,w2.day1_challenge_new_user_count,w2.day2_challenge_new_user_count,
case when d4.day3_challenge_new_user_count is null then 0 else d4.day3_challenge_new_user_count end day3_challenge_new_user_count from
(select w1.challenge_id,w1.challenge_new_user_count,w1.day1_challenge_new_user_count,
case when d3.day2_challenge_new_user_count is null then 0 else d3.day2_challenge_new_user_count end day2_challenge_new_user_count from
(select d1.challenge_id,d1.challenge_new_user_count,
case when d2.day1_challenge_new_user_count is null then 0 else d2.day1_challenge_new_user_count end day1_challenge_new_user_count from
(select a.challenge_id,a.challenge_new_user_count from
(select challenge_id,challenge_new_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${yesterday}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id) d1
left join
(select a.challenge_id,a.challenge_new_user_count as day1_challenge_new_user_count from
(select challenge_id,challenge_new_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${day2}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id) d2
on d1.challenge_id = d2.challenge_id) w1
left join
(select a.challenge_id,a.challenge_new_user_count as day2_challenge_new_user_count from
(select challenge_id,challenge_new_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${day3}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id) d3
on w1.challenge_id = d3.challenge_id) w2
left join
(select a.challenge_id,a.challenge_new_user_count as day3_challenge_new_user_count from
(select challenge_id,challenge_new_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${day4}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id) d4
on w2.challenge_id = d4.challenge_id) w3
left join
(select a.challenge_id,a.challenge_new_user_count as day4_challenge_new_user_count from
(select challenge_id,challenge_new_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${day5}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id) d5
on w3.challenge_id = d5.challenge_id) w4
left join
(select a.challenge_id,a.challenge_new_user_count as day5_challenge_new_user_count from
(select challenge_id,challenge_new_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${day6}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id) d6
on w4.challenge_id = d6.challenge_id) w5
left join
(select a.challenge_id,a.challenge_new_user_count as day6_challenge_new_user_count from
(select challenge_id,challenge_new_user_count from short_video.tbl_ex_short_video_challenge_daily_snapshot where dt = '${day7}') a
join
(select distinct challenge_id from ${tmpTable}) b
on a.challenge_id = b.challenge_id) d7
on w5.challenge_id = d7.challenge_id) t"

create_tmp_table2_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_table2} (stat_date varchar(255),challenge_id VARCHAR(50),week_chart VARCHAR(255))"
execSqlOnMysql "${create_tmp_table2_sql}"
hiveSqlToMysqlNoDelete "${hive_sql5}" "${tmp_mysql_table2}" "stat_date,challenge_id,week_chart"

update_week_chart_sql="UPDATE $mysql_table2 a
JOIN (
  SELECT * FROM $tmp_mysql_table2
  WHERE stat_date='${yesterday}'
) b
ON a.stat_date=b.stat_date AND a.challenge_id=b.challenge_id
SET a.week_chart=b.week_chart
WHERE a.stat_date='${yesterday}';"

execSqlOnMysql "${update_week_chart_sql}"

drop_week_chart_sql="DROP TABLE ${tmp_mysql_table2}"
execSqlOnMysql "${drop_week_chart_sql}"

executeHiveCommand "drop table ${tmpTable}"
