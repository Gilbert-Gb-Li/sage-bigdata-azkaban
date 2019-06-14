#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
echo ${yesterday}
echo "=================================抽取实体 start==================================="

#hive_sql="insert into table takeout.tbl_ex_entity_info_snapshot PARTITION(dt='${yesterday}')
#select case when (t2.license_num is null or t2.license_num = '' or t2.license_num = 'null') then t1.shop_id else t2.license_num end entity_id,t1.shop_id,t2.license_num,t1.take_out_type,
#t1.shop_name,t1.expire_time,t2.expire_time,t2.has_business_license,t1.license_num,t1.org_code,t1.geo_hash,t1.comment_score,t1.negative_comment_rate,
#t1.comment_total_num,t1.negative_comment_num,t2.has_license,t1.province,t1.city
#from (
#select shop_id,shop_name,take_out_type,expire_time,upper(license_num) as license_num,geo_hash,org_code,province,city,comment_score,negative_comment_rate,comment_total_num,negative_comment_num from 
#takeout.tbl_ex_takeout_all_shop where dt='${yesterday}') as t1
#left join
#(select shop_id,take_out_type,expire_time,upper(license_num) as license_num,has_business_license,has_license from takeout.tbl_ex_takeout_shop_all_license where dt='${yesterday}') as t2
#on t1.shop_id=t2.shop_id"

hive_sql_2="insert into table takeout.tbl_ex_entity_info_pre_snapshot PARTITION(dt='${yesterday}')
select a.entity_id,a.shop_id,a.ocr_license_no,a.take_out_type,a.shop_name,a.shop_expire_date,a.license_expire_date,a.is_has_business_license,a.shop_license_no,case when (b.regaddrzl is not null and b.regaddrzl !='' and b.regaddrzl != a.org_code) then b.regaddrzl else a.org_code end org_code,a.geo_hash,a.comment_score,a.negative_comment_rate,a.comment_total_num,a.negative_comment_num,a.has_license,a.province,a.city from 
(select case when (t2.license_num is null or t2.license_num = '' or t2.license_num = 'null') then t1.shop_id else t2.license_num end entity_id,t1.shop_id,t2.license_num as ocr_license_no,t1.take_out_type,
t1.shop_name,t1.expire_time as shop_expire_date,t2.expire_time as license_expire_date ,t2.has_business_license as is_has_business_license,t1.license_num as shop_license_no ,t1.org_code,t1.geo_hash,t1.comment_score,t1.negative_comment_rate,
t1.comment_total_num,t1.negative_comment_num,t2.has_license,t1.province,t1.city
from (
select shop_id,shop_name,take_out_type,expire_time,upper(license_num) as license_num,geo_hash,org_code,province,city,comment_score,negative_comment_rate,comment_total_num,negative_comment_num from 
takeout.tbl_ex_takeout_all_shop where dt='${yesterday}') as t1
left join
(select shop_id,take_out_type,expire_time,upper(license_num) as license_num,has_business_license,has_license from takeout.tbl_ex_takeout_shop_all_license where dt='${yesterday}') as t2
on t1.shop_id=t2.shop_id) as a 
left join 
(select distinct licno,regaddrzl from takeout.tbl_ex_license_info) as b on a.entity_id = b.licno
"


executeHiveCommand "${hive_sql_2}"

hive_2entity_info_sql="
insert into table takeout.tbl_ex_entity_info_snapshot partition(dt='${yesterday}')
select a.entity_id,a.shop_id,a.ocr_license_no,a.take_out_type,a.shop_name,a.shop_expire_date,
a.license_expire_date,a.is_has_business_license,a.shop_license_no,case when (b.org_code is not null and b.org_code != '' ) then b.org_code else a.org_code end org_code,a.geo_hash,a.comment_score,
a.negative_comment_rate,a.comment_total_num,a.negative_comment_num,a.has_license,a.province,a.city
from
(select entity_id,shop_id,ocr_license_no,take_out_type,shop_name,shop_expire_date,
license_expire_date,is_has_business_license,shop_license_no,org_code,geo_hash,comment_score,
negative_comment_rate,comment_total_num,negative_comment_num,has_license,province,city
from takeout.tbl_ex_entity_info_pre_snapshot where dt = '${yesterday}' ) as a left join
(select entity_id,org_code from (
select entity_id,org_code,take_out_type,row_number() over(partition by entity_id,substr(org_code,1,2) order by cast(take_out_type as bigint)) as num from takeout.tbl_ex_entity_info_pre_snapshot where dt = '${yesterday}' ) t  where t.num = 1) as b on a.entity_id = b.entity_id and substr(a.org_code,1,2) = substr(b.org_code,1,2)
"

executeHiveCommand "${hive_2entity_info_sql}"
