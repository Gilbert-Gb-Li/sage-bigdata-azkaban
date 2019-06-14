#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="insert into bigdata.douyin_hot_recommend_details_daily_snapshot partition(dt='${date}')
select record_time,hot_name,source,tags,price,share_url,average_consumption,open_time,hot_id,user_count,grade,web_url,address,telephone,type,rank_type from
(select *,row_number() over (partition by hot_id order by record_time desc) as order_num from (
select t1.record_time,t1.hot_name,t1.source,t1.tags,t1.price,t1.share_url,t1.average_consumption,t1.open_time,t1.hot_id,t1.user_count,t1.grade,t1.web_url,t1.address,t1.telephone,t1.type,t2.rank_type from
(select record_time,hot_name,source,tags,price,share_url,average_consumption,open_time,hot_id,user_count,grade,web_url,address,telephone,type from
(select *,row_number() over (partition by hot_id order by record_time desc) as order_num from (
select record_time,hot_name,source,tags,price,share_url,average_consumption,open_time,hot_id,user_count,grade,web_url,address,telephone,type 
from bigdata.douyin_hot_recommend_details_data_origin_orc where dt = '${date}') as a) as t
where t.order_num = 1) t1
left join
(select record_time,hot_name,hot_id,rank_type from
(select *,row_number() over (partition by hot_id order by record_time desc) as order_num from (
select record_time,hot_name,hot_id,rank_type from bigdata.douyin_hot_recommend_rank_data_origin_orc where dt = '${date}') as a) as t
where t.order_num = 1) t2
on t1.hot_id = t2.hot_id
union all
select record_time,hot_name,source,tags,price,share_url,average_consumption,open_time,hot_id,user_count,grade,web_url,address,telephone,type,hot_type as rank_type from bigdata.douyin_hot_recommend_details_daily_snapshot where dt='${yesterday}'
)as p)as t
where t.order_num =1"

executeHiveCommand "${hive_sql}"