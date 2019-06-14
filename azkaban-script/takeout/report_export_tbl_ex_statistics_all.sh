#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算waimai_statistics_all start #####################"
yesterday=$1
mysql_table="waimai_statistics_all"

hive_sql="
select '${yesterday}',province,city,org_code,take_out_type,
max(shop_num) as shop_num,
max(entity_num) as entity_num,
max(licence_num) as licence_num,
max(warn_num) as warn_num,
max(suspicious_num) as suspicious_num,
max(suspicious_entity_num) as suspicious_entity_num
from
(
select dt,province,city,org_code,take_out_type,count(distinct shop_id) as shop_num,count(distinct entity_id) as entity_num,count(distinct case when (ocr_license_no !='') then ocr_license_no else NULL end) as licence_num,
0 as warn_num,0 as suspicious_num,0 as suspicious_entity_num from
(select dt,org_code,province,city,take_out_type,entity_id,shop_id,ocr_license_no from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
union all
select dt,org_code,province,city,'0' as take_out_type,entity_id,shop_id,ocr_license_no from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
) t
group by dt,province,city,org_code,take_out_type

union all

select dt,province,'' as city,org_code,take_out_type,count(distinct shop_id) as shop_num,count(distinct entity_id) as entity_num,count(distinct case when (ocr_license_no !='') then ocr_license_no else NULL end) as licence_num,
0 as warn_num,0 as suspicious_num,0 as suspicious_entity_num from
(select dt,substr(org_code,1,2) as org_code,province,take_out_type,entity_id,shop_id,ocr_license_no from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
union all
select dt,substr(org_code,1,2) as org_code,province,'0' as take_out_type,entity_id,shop_id,ocr_license_no from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
) t
group by dt,province,org_code,take_out_type

union all

select dt,province,city,org_code,take_out_type,count(distinct shop_id) as shop_num,count(distinct entity_id) as entity_num,count(distinct case when (ocr_license_no !='') then ocr_license_no else NULL end) as licence_num,
0 as warn_num,0 as suspicious_num,0 as suspicious_entity_num from
(select dt,substr(org_code,1,4) as org_code,province,city,take_out_type,entity_id,shop_id,ocr_license_no from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
union all
select dt,substr(org_code,1,4) as org_code,province,city,'0' as take_out_type,entity_id,shop_id,ocr_license_no from takeout.tbl_ex_entity_info_snapshot where dt = '${yesterday}'
) t
group by dt,province,city,org_code,take_out_type

union all

select dt,province,city,org_code,take_out_type,
0 as shop_num,0 as entity_num,0 as licence_num,count(distinct warn_entity_num) as warn_num,
count(distinct info_error_num) + count(distinct cart_error_num) as suspicious_num,
count(distinct suspicious_entity_num) as suspicious_entity_num from
(
select dt,province,city,org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}') t
group by dt,province,city,org_code,take_out_type

union all

select dt,province,city,org_code,take_out_type,
0 as shop_num,0 as entity_num,0 as licence_num,count(distinct warn_entity_num) as warn_num,
count(distinct info_error_num) + count(distinct cart_error_num) as suspicious_num,
count(distinct suspicious_entity_num) as suspicious_entity_num from
(
select dt,province,city,substr(org_code,1,4) as org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,substr(org_code,1,4) as org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}') t
group by dt,province,city,org_code,take_out_type

union all

select dt,province,'' as city,org_code,take_out_type,
0 as shop_num,0 as entity_num,0 as licence_num,count(distinct warn_entity_num) as warn_num,
count(distinct info_error_num) + count(distinct cart_error_num) as suspicious_num,
count(distinct suspicious_entity_num) as suspicious_entity_num from
(
select dt,province,substr(org_code,1,2) as org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,substr(org_code,1,2) as org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}') t
group by dt,province,org_code,take_out_type
) t where province is not null and province!=''
group by province,city,org_code,take_out_type;"

hiveSqlToTakeoutMysql "${hive_sql}" "${yesterday}" "${mysql_table}" "date,province,city,orgcode,waimai_type,shop_num,entity_num,licence_num,warn_num,suspicious_num,suspicious_entity_num" "date"

echo "############### 计算waimai_statistics_all end #####################"
