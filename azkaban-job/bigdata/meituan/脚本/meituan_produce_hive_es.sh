#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
date=$1
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
weekday=`date -d "-6 day $date" +%Y-%m-%d`
month2day=`date -d "-29 day $date" +%Y-%m-%d`
echo ${yesterday}
echo ${weekday}
echo ${month2day}
echo "=================================hive_result_wide映射到wide_es start==================================="
sql1="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
	add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
	insert into table bigdata.meituan_result_wide_day_snapshot_es 
	select a.*,b.*,c.*,d.*, '${yesterday}',
	'meituan' 
	from 
	(select new_shop_num,new_food_num,new_not_foot_num,meituan_special_num,meituan_quick_num,self_take_num from bigdata.takeaway_meituan_result_day_week_month_snapshot  where dt='${yesterday}' and time_cecle = 1) a,
	(select new_shop_num,new_food_num,new_not_foot_num,meituan_special_num,meituan_quick_num,self_take_num from bigdata.takeaway_meituan_result_day_week_month_snapshot  where dt='${yesterday}' and time_cecle = 2) b,
	(select new_shop_num,new_food_num,new_not_foot_num,meituan_special_num,meituan_quick_num,self_take_num from bigdata.takeaway_meituan_result_day_week_month_snapshot  where dt='${yesterday}' and time_cecle = 3) c,
	(select shop_count_platform,order_num_platform,meituan_special_num,meituan_quick_num,self_take_num,total_transactions_platform from bigdata.takeaway_meituan_result_order_platform_snapshot where dt='${yesterday}') d;"
echo "=================================商家账面交易总额日表映射到es start==================================="
sql2="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
	add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
	insert into table bigdata.takeaway_meituan_transactions_shop_day_es 
	select a.shop_id,a.order_num,a.total_transactions_shop,'${yesterday}',
	'meituan' 
	from bigdata.takeaway_meituan_transactions_shop_result_snapshot a where a.dt='${yesterday}';"

executeHiveCommand "${sql1}"
executeHiveCommand "${sql2}"

