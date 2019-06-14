#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
date_reduce_1=`date -d "-1 day $yesterday" +%Y-%m-%d`
stat_date=`date -d "$yesterday" +%Y%m%d`
month1=`date -d "${yesterday}" +%Y-%m`
month1_01=`date -d "${month1}-01" +%Y-%m-%d`
month1_01_1=`date -d "${month1}-01" +%Y%m%d`
month1_01_reduce_1=`date -d "-1 day $month1_01" +%Y-%m-%d`
month2=`date -d "${month1_01_reduce_1}" +%Y-%m`
month2_01=`date -d "${month2}-01" +%Y-%m-%d`
month2_01_1=`date -d "${month2}-01" +%Y%m%d`

echo "++++++++++++++++++++++++++++++++计算生成头部集用户数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql1="insert into bigdata.douyin_user_r_t_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'user' as meta_table_name,s1.record_time,s1.set_type,s1.user_id,s1.douyin_id,s1.age,s1.sex,s1.location,s1.school,s1.certificate_type,s1.certificate_info,s1.shop_window,case when s3.taobao_shop_total is null then 0 else s3.taobao_shop_total end taobao_shop_total,case when s3.init_shop_total is null then 0 else s3.init_shop_total end init_shop_total,case when s3.consume_count is null then 0 else s3.consume_count end consume_count,s1.dynamic_count,
s1.like_video_count,s1.new_like_video_count,s1.like_count,s1.new_like_count,s1.follower_count,s1.new_follower_count,s1.following_count,s1.new_following_count,s1.short_video_count,s1.new_video_count,case when s2.challenge_video_count is null then 0 else s2.challenge_video_count end challenge_video_count,case when s2.new_challenge_video_count is null then 0 else s2.new_challenge_video_count end new_challenge_video_count,case when s4.comment_count is null then 0 else s4.comment_count end comment_count,case when s4.new_comment_count is null then 0 else s4.new_comment_count end new_comment_count,'${stat_date}' as extract_date from
(select '1' as set_type,t1.record_time,t1.user_id,t1.douyin_id,t1.age,t1.sex,t1.location,t1.school,t1.certificate_type,t1.certificate_info,t1.shop_window,t1.dynamic_count,
t1.like_video_count,case when t2.like_video_count is null or t2.like_video_count = '' or t2.like_video_count = -1 then t1.like_video_count else t1.like_video_count - t2.like_video_count end new_like_video_count,
t1.like_count,case when t2.like_count is null or t2.like_count = '' or t2.like_count = -1 then t1.like_count else t1.like_count - t2.like_count end new_like_count,
t1.follower_count,case when t2.follower_count is null or t2.follower_count = '' or t2.follower_count = -1 then t1.follower_count else t1.follower_count - t2.follower_count end new_follower_count,
t1.following_count,case when t2.following_count is null or t2.following_count = '' or t2.following_count = -1 then t1.following_count else t1.following_count - t2.following_count end new_following_count,
t1.short_video_count,case when t2.short_video_count is null or t2.short_video_count = '' or t2.short_video_count = -1  then t1.short_video_count else t1.short_video_count - t2.short_video_count end new_video_count from
(select a.record_time,a.user_id,a.douyin_id,a.age,a.sex,a.location,a.school,a.certificate_type,a.certificate_info,a.shop_window,a.dynamic_count,a.like_video_count,a.like_count,a.follower_count,a.following_count,a.short_video_count from
(select record_time,user_id,douyin_id,age,sex,location,school,certificate_type,certificate_info,shop_window,dynamic_count,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.user_id = b.user_id) t1
left join
(select a.user_id,a.like_video_count,a.like_count,a.follower_count,a.following_count,a.short_video_count from
(select user_id,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.user_id = b.user_id) t2
on t1.user_id = t2.user_id) s1
left join
(select t1.author_id,case when t2.challenge_video_count is null or t2.challenge_video_count = '' or t2.challenge_video_count = -1 then t1.challenge_video_count else t1.challenge_video_count - t2.challenge_video_count end new_challenge_video_count,t1.challenge_video_count from 
(select a.author_id,count(distinct a.short_video_id) as challenge_video_count from
(select author_id,short_video_id,challenge_id from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}' and challenge_id is not null and challenge_id != '') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.author_id = b.user_id
group by a.author_id) t1
left join
(select a.author_id,count(distinct a.short_video_id) as challenge_video_count from
(select author_id,short_video_id,challenge_id from bigdata.douyin_video_daily_snapshot where dt = '${date_reduce_1}' and challenge_id is not null and challenge_id != '') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.author_id = b.user_id
group by a.author_id) t2
on t1.author_id = t2.author_id) s2
on s1.user_id = s2.author_id
left join
(select a.store_id,sum(transaction_record) as consume_count,count(distinct init_url) as init_shop_total,count(distinct taobao_url) as taobao_shop_total from
(select store_id,transaction_record,case when goods_url_type = 1 then store_id end init_url,case when goods_url_type = 2 then store_id end taobao_url from bigdata.douyin_shop_window_goods_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.store_id = b.user_id
group by a.store_id) s3
on s1.user_id = s3.store_id
left join
(select t1.user_id,t1.comment_count,case when t2.comment_count is null or t2.comment_count = '' then t1.comment_count else t1.comment_count - t2.comment_count end new_comment_count from
(select a.user_id,count(distinct a.comment_id) as comment_count from
(select user_id,comment_id from bigdata.douyin_video_comment_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.user_id = b.user_id
group by a.user_id) t1
left join
(select a.user_id,count(distinct a.comment_id) as comment_count from
(select user_id,comment_id from bigdata.douyin_video_comment_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_header_user_data_orc where dt = '${yesterday}') b
on a.user_id = b.user_id
group by a.user_id) t2
on t1.user_id = t2.user_id) s4
on s1.user_id = s4.user_id;"

executeHiveCommand "${hive_sql1}"

echo "++++++++++++++++++++++++++++++++计算生成当月抽样集用户数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql2="insert into bigdata.douyin_user_r_t_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'user' as meta_table_name,s1.record_time,s1.set_type,s1.user_id,s1.douyin_id,s1.age,s1.sex,s1.location,s1.school,s1.certificate_type,s1.certificate_info,s1.shop_window,case when s3.taobao_shop_total is null then 0 else s3.taobao_shop_total end taobao_shop_total,case when s3.init_shop_total is null then 0 else s3.init_shop_total end init_shop_total,case when s3.consume_count is null then 0 else s3.consume_count end consume_count,s1.dynamic_count,
s1.like_video_count,s1.new_like_video_count,s1.like_count,s1.new_like_count,s1.follower_count,s1.new_follower_count,s1.following_count,s1.new_following_count,s1.short_video_count,s1.new_video_count,case when s2.challenge_video_count is null then 0 else s2.challenge_video_count end challenge_video_count,case when s2.new_challenge_video_count is null then 0 else s2.new_challenge_video_count end new_challenge_video_count,case when s4.comment_count is null then 0 else s4.comment_count end comment_count,case when s4.new_comment_count is null then 0 else s4.new_comment_count end new_comment_count,'${month1_01_1}' as extract_date from
(select '2' as set_type,t1.record_time,t1.user_id,t1.douyin_id,t1.age,t1.sex,t1.location,t1.school,t1.certificate_type,t1.certificate_info,t1.shop_window,t1.dynamic_count,
t1.like_video_count,case when t2.like_video_count is null or t2.like_video_count = '' or t2.like_video_count = -1 then t1.like_video_count else t1.like_video_count - t2.like_video_count end new_like_video_count,
t1.like_count,case when t2.like_count is null or t2.like_count = '' or t2.like_count = -1 then t1.like_count else t1.like_count - t2.like_count end new_like_count,
t1.follower_count,case when t2.follower_count is null or t2.follower_count = '' or t2.follower_count = -1 then t1.follower_count else t1.follower_count - t2.follower_count end new_follower_count,
t1.following_count,case when t2.following_count is null or t2.following_count = '' or t2.following_count = -1 then t1.following_count else t1.following_count - t2.following_count end new_following_count,
t1.short_video_count,case when t2.short_video_count is null or t2.short_video_count = '' or t2.short_video_count = -1  then t1.short_video_count else t1.short_video_count - t2.short_video_count end new_video_count from
(select a.record_time,a.user_id,a.douyin_id,a.age,a.sex,a.location,a.school,a.certificate_type,a.certificate_info,a.shop_window,a.dynamic_count,a.like_video_count,a.like_count,a.follower_count,a.following_count,a.short_video_count from
(select record_time,user_id,douyin_id,age,sex,location,school,certificate_type,certificate_info,shop_window,dynamic_count,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month1_01}') b
on a.user_id = b.user_id) t1
left join
(select a.user_id,a.like_video_count,a.like_count,a.follower_count,a.following_count,a.short_video_count from
(select user_id,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month1_01}') b
on a.user_id = b.user_id) t2
on t1.user_id = t2.user_id) s1
left join
(select t1.author_id,case when t2.challenge_video_count is null or t2.challenge_video_count = '' or t2.challenge_video_count = -1 then t1.challenge_video_count else t1.challenge_video_count - t2.challenge_video_count end new_challenge_video_count,t1.challenge_video_count from 
(select a.author_id,count(distinct a.short_video_id) as challenge_video_count from
(select author_id,short_video_id,challenge_id from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}' and challenge_id is not null and challenge_id != '') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month1_01}') b
on a.author_id = b.user_id
group by a.author_id) t1
left join
(select a.author_id,count(distinct a.short_video_id) as challenge_video_count from
(select author_id,short_video_id,challenge_id from bigdata.douyin_video_daily_snapshot where dt = '${date_reduce_1}' and challenge_id is not null and challenge_id != '') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month1_01}') b
on a.author_id = b.user_id
group by a.author_id) t2
on t1.author_id = t2.author_id) s2
on s1.user_id = s2.author_id
left join
(select a.store_id,sum(transaction_record) as consume_count,count(distinct init_url) as init_shop_total,count(distinct taobao_url) as taobao_shop_total from
(select store_id,transaction_record,case when goods_url_type = 1 then store_id end init_url,case when goods_url_type = 2 then store_id end taobao_url from bigdata.douyin_shop_window_goods_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month1_01}') b
on a.store_id = b.user_id
group by a.store_id) s3
on s1.user_id = s3.store_id
left join
(select t1.user_id,t1.comment_count,case when t2.comment_count is null or t2.comment_count = '' then t1.comment_count else t1.comment_count - t2.comment_count end new_comment_count from
(select a.user_id,count(distinct a.comment_id) as comment_count from
(select user_id,comment_id from bigdata.douyin_video_comment_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month1_01}') b
on a.user_id = b.user_id
group by a.user_id) t1
left join
(select a.user_id,count(distinct a.comment_id) as comment_count from
(select user_id,comment_id from bigdata.douyin_video_comment_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month1_01}') b
on a.user_id = b.user_id
group by a.user_id) t2
on t1.user_id = t2.user_id) s4
on s1.user_id = s4.user_id;"

executeHiveCommand "${hive_sql2}"

echo "++++++++++++++++++++++++++++++++计算生成上月抽样集用户数据中间表++++++++++++++++++++++++++++++++++++++"
hive_sql3="insert into bigdata.douyin_user_r_t_data partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'user' as meta_table_name,s1.record_time,s1.set_type,s1.user_id,s1.douyin_id,s1.age,s1.sex,s1.location,s1.school,s1.certificate_type,s1.certificate_info,s1.shop_window,case when s3.taobao_shop_total is null then 0 else s3.taobao_shop_total end taobao_shop_total,case when s3.init_shop_total is null then 0 else s3.init_shop_total end init_shop_total,case when s3.consume_count is null then 0 else s3.consume_count end consume_count,s1.dynamic_count,
s1.like_video_count,s1.new_like_video_count,s1.like_count,s1.new_like_count,s1.follower_count,s1.new_follower_count,s1.following_count,s1.new_following_count,s1.short_video_count,s1.new_video_count,case when s2.challenge_video_count is null then 0 else s2.challenge_video_count end challenge_video_count,case when s2.new_challenge_video_count is null then 0 else s2.new_challenge_video_count end new_challenge_video_count,case when s4.comment_count is null then 0 else s4.comment_count end comment_count,case when s4.new_comment_count is null then 0 else s4.new_comment_count end new_comment_count,'${month2_01_1}' as extract_date from
(select '2' as set_type,t1.record_time,t1.user_id,t1.douyin_id,t1.age,t1.sex,t1.location,t1.school,t1.certificate_type,t1.certificate_info,t1.shop_window,t1.dynamic_count,
t1.like_video_count,case when t2.like_video_count is null or t2.like_video_count = '' or t2.like_video_count = -1 then t1.like_video_count else t1.like_video_count - t2.like_video_count end new_like_video_count,
t1.like_count,case when t2.like_count is null or t2.like_count = '' or t2.like_count = -1 then t1.like_count else t1.like_count - t2.like_count end new_like_count,
t1.follower_count,case when t2.follower_count is null or t2.follower_count = '' or t2.follower_count = -1 then t1.follower_count else t1.follower_count - t2.follower_count end new_follower_count,
t1.following_count,case when t2.following_count is null or t2.following_count = '' or t2.following_count = -1 then t1.following_count else t1.following_count - t2.following_count end new_following_count,
t1.short_video_count,case when t2.short_video_count is null or t2.short_video_count = '' or t2.short_video_count = -1  then t1.short_video_count else t1.short_video_count - t2.short_video_count end new_video_count from
(select a.record_time,a.user_id,a.douyin_id,a.age,a.sex,a.location,a.school,a.certificate_type,a.certificate_info,a.shop_window,a.dynamic_count,a.like_video_count,a.like_count,a.follower_count,a.following_count,a.short_video_count from
(select record_time,user_id,douyin_id,age,sex,location,school,certificate_type,certificate_info,shop_window,dynamic_count,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month2_01}') b
on a.user_id = b.user_id) t1
left join
(select a.user_id,a.like_video_count,a.like_count,a.follower_count,a.following_count,a.short_video_count from
(select user_id,like_video_count,like_count,follower_count,following_count,short_video_count from bigdata.douyin_user_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month2_01}') b
on a.user_id = b.user_id) t2
on t1.user_id = t2.user_id) s1
left join
(select t1.author_id,case when t2.challenge_video_count is null or t2.challenge_video_count = '' or t2.challenge_video_count = -1 then t1.challenge_video_count else t1.challenge_video_count - t2.challenge_video_count end new_challenge_video_count,t1.challenge_video_count from 
(select a.author_id,count(distinct a.short_video_id) as challenge_video_count from
(select author_id,short_video_id,challenge_id from bigdata.douyin_video_daily_snapshot where dt = '${yesterday}' and challenge_id is not null and challenge_id != '') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month2_01}') b
on a.author_id = b.user_id
group by a.author_id) t1
left join
(select a.author_id,count(distinct a.short_video_id) as challenge_video_count from
(select author_id,short_video_id,challenge_id from bigdata.douyin_video_daily_snapshot where dt = '${date_reduce_1}' and challenge_id is not null and challenge_id != '') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month2_01}') b
on a.author_id = b.user_id
group by a.author_id) t2
on t1.author_id = t2.author_id) s2
on s1.user_id = s2.author_id
left join
(select a.store_id,sum(transaction_record) as consume_count,count(distinct init_url) as init_shop_total,count(distinct taobao_url) as taobao_shop_total from
(select store_id,transaction_record,case when goods_url_type = 1 then store_id end init_url,case when goods_url_type = 2 then store_id end taobao_url from bigdata.douyin_shop_window_goods_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month2_01}') b
on a.store_id = b.user_id
group by a.store_id) s3
on s1.user_id = s3.store_id
left join
(select t1.user_id,t1.comment_count,case when t2.comment_count is null or t2.comment_count = '' then t1.comment_count else t1.comment_count - t2.comment_count end new_comment_count from
(select a.user_id,count(distinct a.comment_id) as comment_count from
(select user_id,comment_id from bigdata.douyin_video_comment_daily_snapshot where dt = '${yesterday}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month2_01}') b
on a.user_id = b.user_id
group by a.user_id) t1
left join
(select a.user_id,count(distinct a.comment_id) as comment_count from
(select user_id,comment_id from bigdata.douyin_video_comment_daily_snapshot where dt = '${date_reduce_1}') a
join
(select user_id from bigdata.douyin_sampling_user_data_orc where dt = '${month2_01}') b
on a.user_id = b.user_id
group by a.user_id) t2
on t1.user_id = t2.user_id) s4
on s1.user_id = s4.user_id;"

executeHiveCommand "${hive_sql3}"

echo "++++++++++++++++++++++++++++++++导出用户数据到ES++++++++++++++++++++++++++++++++++++++"
hive_sql3="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;insert into bigdata.douyin_user_r_t_es_data partition(dt='${yesterday}')
select '${stat_date}',unix_timestamp(dt, 'yyyy-MM-dd')*1000,meta_app_name,meta_table_name,record_time,set_type,user_id,douyin_id,age,sex,location,school,certificate_type,certificate_info,shop_window,taobao_shop_total,init_shop_total,consume_record,dynamic_count,like_video_count,new_like_video_count,like_count,new_like_count,follower_count,new_follower_count,following_count,new_following_count,short_video_count,new_video_count,challenge_video_count,new_challenge_video_count,comment_count,new_comment_count,extract_date
from bigdata.douyin_user_r_t_data where dt = '${yesterday}'"
executeHiveCommand "${hive_sql3}"