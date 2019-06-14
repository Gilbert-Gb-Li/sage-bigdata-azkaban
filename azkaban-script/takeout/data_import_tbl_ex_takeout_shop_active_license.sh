#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算活跃license信息 start #####################"
yesterday=$1

hive_sql="insert into table takeout.tbl_ex_takeout_shop_active_license PARTITION(dt='${yesterday}')
select shop_id,take_out_type,expire_time,license_num,has_business_license,has_license from 
(select t.*,row_number() over (partition by shop_id order by record_time desc) num from(
select record_time,shop_id,take_out_type,expire_time,license_num,has_business_license,has_license 
from ias.tbl_ex_takeout_shop_license_origin_orc where dt = '${yesterday}' and license_num != 'null') as t) r
where r.num = 1;"

executeHiveCommand "${hive_sql}"

echo "############### 计算活跃license信息 end #####################"
