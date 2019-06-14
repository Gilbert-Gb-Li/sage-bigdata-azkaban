#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
stat_date=`date -d "$yesterday" +%Y%m%d`
date_add_1=`date -d "+1 day $yesterday" +%Y-%m-%d`
date_reduce_6=`date -d "-6 day $yesterday" +%Y-%m-%d`
date_reduce_7=`date -d "-7 day $yesterday" +%Y-%m-%d`
month1_01_reduce_13=`date -d "-13 day $yesterday" +%Y-%m-%d`
week=`date -d "${date_add_1}" +%w`
echo "周：${week}"
month=`date -d "${yesterday}" +%Y%m`
echo "月格式1：${month}"
month1=`date -d "${yesterday}" +%Y-%m`
echo "月格式2：${month1}"
month1_01=`date -d "${month1}-01" +%Y-%m-%d`
echo "月第一天：${month1_01}"
month1_01_reduce_1=`date -d "-1 day $month1_01" +%Y-%m-%d`
echo "上月最后一天：${month1_01_reduce_1}"
month1_01_reduce_1_1=`date -d "$month1_01_reduce_1" +%Y%m%d`
echo "上月最后一天：${month1_01_reduce_1_1}"
month2=`date -d "${month1_01_reduce_1}" +%Y-%m`
echo "上月格式2：${month2}"
month2_01=`date -d "${month2}-01" +%Y-%m-%d`
echo "上月第一天：${month2_01}"
day=`date -d "${date_add_1}" +%d`

hive_sql1="insert into bigdata.douyin_remain_data_snapshot partition(dt='${yesterday}')
select 'douyin' as meta_app_name,'remain' as meta_table_name,'${month}' as stat_date,3 as remain_type,'3' as set_type,s1.like_remain,s1.like_origin,
s2.follower_remain,s2.follower_origin,s3.comment_remain,s3.comment_origin,s4.video_remain,s4.video_origin,'${month1_01_reduce_1_1}' as extract_date from 
(select t1.remain_count as like_remain,t2.remain_count as like_origin from
(select count(distinct a.user_id) as remain_count from
(select user_id from bigdata.douyin_user_all_data where dt = '${yesterday}' and new_like_video_count > 0) a
join
(select user_id from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_like_video_count > 0) b
on a.user_id = b.user_id) t1
left join
(select count(distinct user_id) as remain_count from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_like_video_count > 0) t2) s1
left join
(select t1.remain_count as follower_remain,t2.remain_count as follower_origin from
(select count(distinct a.user_id) as remain_count from
(select user_id from bigdata.douyin_user_all_data where dt = '${yesterday}' and new_following_count > 0) a
join
(select user_id from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_following_count > 0) b
on a.user_id = b.user_id) t1
left join
(select count(distinct user_id) as remain_count from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_following_count > 0) t2) s2
left join
(select t1.remain_count as comment_remain,t2.remain_count as comment_origin from
(select count(distinct a.user_id) as remain_count from
(select user_id from bigdata.douyin_user_all_data where dt = '${yesterday}' and new_comment_count > 0) a
join
(select user_id from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_comment_count > 0) b
on a.user_id = b.user_id) t1
left join
(select count(distinct user_id) as remain_count from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_comment_count > 0) t2) s3
left join
(select t1.remain_count as video_remain,t2.remain_count as video_origin from
(select count(distinct a.user_id) as remain_count from
(select user_id from bigdata.douyin_user_all_data where dt = '${yesterday}' and new_video_count > 0) a
join
(select user_id from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_video_count > 0) b
on a.user_id = b.user_id) t1
left join
(select count(distinct user_id) as remain_count from bigdata.douyin_user_all_data where dt = '${month1_01_reduce_1}' and new_video_count > 0) t2) s4;"

executeHiveCommand "${hive_sql1}"

hive_sql2="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;insert into bigdata.douyin_monthly_remain_es_data partition(dt='${yesterday}')
select unix_timestamp(dt, 'yyyy-MM-dd')*1000,meta_app_name,meta_table_name,set_type,stat_date,like_remain,like_origin,follower_remain,follower_origin,comment_remain,comment_origin,video_remain,video_origin,extract_date 
from bigdata.douyin_remain_data_snapshot where dt = '${yesterday}' and remain_type = 3 and set_type = '3'"


executeHiveCommand "${hive_sql2}"