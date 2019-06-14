#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
date_reduce_1=`date -d "-1 day $yesterday" +%Y-%m-%d`
stat_date=`date -d "$yesterday" +%Y%m%d`
year=`date -d "$yesterday" +%Y`
month=`date -d "$yesterday" +%m`


#创建临时表
tmp_table_name_suffix1="tmp_user_table"
hive_sql1="select t1.record_time,t1.user_id,t1.douyin_id,t1.age,t1.sex,t1.location,t1.school,t1.certificate_type,t1.certificate_info,t1.shop_window,t1.dynamic_count,
t1.like_video_count,case when t2.like_video_count is null or t2.like_video_count = '' or t2.like_video_count = -1 then t1.like_video_count else t1.like_video_count - t2.like_video_count end new_like_video_count,
t1.like_count,case when t2.like_count is null or t2.like_count = '' or t2.like_count = -1 then t1.like_count else t1.like_count - t2.like_count end new_like_count,
t1.follower_count,case when t2.follower_count is null or t2.follower_count = '' or t2.follower_count = -1 then t1.follower_count else t1.follower_count - t2.follower_count end new_follower_count,
t1.following_count,case when t2.following_count is null or t2.following_count = '' or t2.following_count = -1 then t1.following_count else t1.following_count - t2.following_count end new_following_count,
t1.short_video_count,case when t2.short_video_count is null or t2.short_video_count = '' or t2.short_video_count = -1 then t1.short_video_count else t1.short_video_count - t2.short_video_count end new_video_count from
(select record_time,user_id,douyin_id,age,sex,location,school,certificate_type,certificate_info,shop_window,dynamic_count,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${yesterday}') t1
left join
(select user_id,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${year}-${month}-01') t2
on t1.user_id = t2.user_id;"
tmp_user_table=`hiveSqlToTmpHive "${hive_sql1}" "${tmp_table_name_suffix1}"`

tmp_table_name_suffix2="tmp_video_table1"
hive_sql2="select author_id,count(distinct short_video_id) as challenge_video_count from bigdata.douyin_video_daily_snapshot where dt = '${year}-${month}-01' and challenge_id is not null and challenge_id != '' group by author_id;"
tmp_video_table1=`hiveSqlToTmpHive "${hive_sql2}" "${tmp_table_name_suffix2}"`

tmp_table_name_suffix5="tmp_video_table2"
hive_sql5="select t1.author_id,case when t2.challenge_video_count is null or t2.challenge_video_count = '' or t2.challenge_video_count = -1 then t1.challenge_video_count else t1.challenge_video_count - t2.challenge_video_count end new_challenge_video_count,t1.challenge_video_count from
(select author_id,count(distinct short_video_id) as challenge_video_count from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}' and challenge_id is not null and challenge_id != '' group by author_id) t1
left join
$tmp_video_table1 t2
on t1.author_id = t2.author_id;"
tmp_video_table2=`hiveSqlToTmpHive "${hive_sql5}" "${tmp_table_name_suffix5}"`

tmp_table_name_suffix3="tmp_shop_table"
hive_sql3="select store_id,sum(transaction_record) as consume_count,count(distinct init_url) as init_shop_total,count(distinct taobao_url) as taobao_shop_total from
(select store_id,transaction_record,case when goods_url_type = 1 then store_id end init_url,case when goods_url_type = 2 then store_id end taobao_url from bigdata.douyin_shop_window_goods_daily_snapshot where dt = '${yesterday}') a
group by a.store_id;"
tmp_shop_table=`hiveSqlToTmpHive "${hive_sql3}" "${tmp_table_name_suffix3}"`

tmp_table_name_suffix4="tmp_comment_table"
hive_sql4="select user_id,comment_count1 as comment_count,comment_count1-comment_count2 as new_comment_count from
(select user_id,MAX(comment_count1) as comment_count1,MAX(comment_count2) as comment_count2 from
(select user_id,if(dt = '${yesterday}',comment_count,0) as comment_count1,if(dt = '${year}-${month}-01',comment_count,0) as comment_count2 from
(select '${yesterday}' as dt,user_id,count(distinct comment_id) as comment_count from bigdata.douyin_video_comment_daily_snapshot where dt = '${yesterday}' group by user_id
union all
select '${year}-${month}-01' as dt,user_id,count(distinct comment_id) as comment_count from bigdata.douyin_video_comment_daily_snapshot where dt = '${year}-${month}-01' group by user_id) t) s
group by user_id) t;"
tmp_comment_table=`hiveSqlToTmpHive "${hive_sql4}" "${tmp_table_name_suffix4}"`

hive_sql6="insert into bigdata.douyin_user_all_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'user' as meta_table_name,s1.record_time,s1.user_id,s1.douyin_id,s1.age,s1.sex,s1.location,s1.school,s1.certificate_type,s1.certificate_info,s1.shop_window,case when s3.taobao_shop_total is null then 0 else s3.taobao_shop_total end taobao_shop_total,case when s3.init_shop_total is null then 0 else s3.init_shop_total end init_shop_total,case when s3.consume_count is null then 0 else s3.consume_count end consume_count,s1.dynamic_count,
s1.like_video_count,s1.new_like_video_count,s1.like_count,s1.new_like_count,s1.follower_count,s1.new_follower_count,s1.following_count,s1.new_following_count,s1.short_video_count,s1.new_video_count,case when s2.challenge_video_count is null then 0 else s2.challenge_video_count end challenge_video_count,case when s2.new_challenge_video_count is null then 0 else s2.new_challenge_video_count end new_challenge_video_count,case when s4.comment_count is null then 0 else s4.comment_count end comment_count,case when s4.new_comment_count is null then 0 else s4.new_comment_count end new_comment_count from
$tmp_user_table s1
left join
$tmp_video_table2 s2
on s1.user_id = s2.author_id
left join
$tmp_shop_table s3
on s1.user_id = s3.store_id
left join
$tmp_comment_table s4
on s1.user_id = s4.user_id;"

executeHiveCommand "${hive_sql6}"

drop_table_sql1="DROP TABLE $tmp_user_table"

executeHiveCommand "${drop_table_sql1}"
drop_table_sql2="DROP TABLE $tmp_video_table1"

executeHiveCommand "${drop_table_sql2}"
drop_table_sql3="DROP TABLE $tmp_video_table2"

executeHiveCommand "${drop_table_sql3}"
drop_table_sql4="DROP TABLE $tmp_shop_table"

executeHiveCommand "${drop_table_sql4}"
drop_table_sql5="DROP TABLE $tmp_comment_table"

executeHiveCommand "${drop_table_sql5}"

echo "++++++++++++++++++++++++++++++++导出全量用户数据到ES++++++++++++++++++++++++++++++++++++++"
hive_sql7="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;insert into bigdata.douyin_user_all_es_data partition(dt='${yesterday}')
select substr(${stat_date},1,6) as stat_month,unix_timestamp(dt, 'yyyy-MM-dd')*1000,meta_app_name,meta_table_name,record_time,user_id,douyin_id,age,sex,location,school,certificate_type,certificate_info,shop_window,taobao_shop_total,init_shop_total,consume_record,dynamic_count,like_video_count,new_like_video_count,like_count,new_like_count,follower_count,new_follower_count,following_count,new_following_count,short_video_count,new_video_count,challenge_video_count,new_challenge_video_count,comment_count,new_comment_count
from bigdata.douyin_user_all_data where dt = '${yesterday}'"
executeHiveCommand "${hive_sql7}"