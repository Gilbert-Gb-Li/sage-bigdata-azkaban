#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算新增商户 start #####################"
yesterday=$1
the_day_before_yesterday=`date -d "-1 day ${yesterday}" +%Y-%m-%d`

hive_sql="insert into table takeout.tbl_ex_takeout_new_shop PARTITION(dt='${yesterday}')
select a.shop_id,a.take_out_type from
(select shop_id,take_out_type from takeout.tbl_ex_takeout_active_shop where dt = '${yesterday}' and (shop_cert_photo1 is not null and shop_cert_photo1 !='' 
and shop_cert_photo2 is not null and shop_cert_photo2 !='' and phone is not null and phone!='')) a
left join
(select shop_id,take_out_type from takeout.tbl_ex_takeout_all_shop where dt = '${the_day_before_yesterday}' and (shop_cert_photo1 is not null and shop_cert_photo1 !='' 
and shop_cert_photo2 is not null and shop_cert_photo2 !='' and phone is not null and phone!='')) b
on a.shop_id = b.shop_id
where b.shop_id is null;"

executeHiveCommand "${hive_sql}"

echo "############### 计算新增商户 end #####################"
