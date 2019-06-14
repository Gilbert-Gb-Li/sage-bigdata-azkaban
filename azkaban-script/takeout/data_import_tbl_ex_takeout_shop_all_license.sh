#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算全量license信息 start #####################"
yesterday=$1
echo $yesterday
the_day_before_yesterday=`date -d "-1 day ${yesterday}" +%Y-%m-%d`
echo $the_day_before_yesterday

hive_sql="insert into table takeout.tbl_ex_takeout_shop_all_license PARTITION(dt='${yesterday}')
select distinct shop_id,take_out_type,expire_time,license_num,has_business_license,has_license from (
select t.*,row_number() over (partition by shop_id order by dt desc) num from(
select * from takeout.tbl_ex_takeout_shop_active_license where dt = '${yesterday}'
union
select * from takeout.tbl_ex_takeout_shop_all_license where dt = '${the_day_before_yesterday}') as t
) r
where r.num = 1;"

executeHiveCommand "${hive_sql}"
echo "############### 计算全量license信息 end #####################"