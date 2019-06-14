#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算waimai_statistics_total start #####################"
yesterday=$1
mysql_table="waimai_statistics_total"

hive_sql="
select '${yesterday}',province,city,org_code,take_out_type,max(warn_num) as warn_num,max(suspicious_num) as suspicious_num,max(normal_num) as normal_num,max(registration_num) as registration_num,max(entity_num) as entity_num from
(select province,city,org_code,take_out_type,count(distinct warn_num) as warn_num,count(distinct suspicious_num) as suspicious_num,count(distinct normal_num) as normal_num,0 as registration_num,0 as entity_num from
(select province,city,org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_num,
case when question_type in ('11','12') then entity_id else null end suspicious_num,
case when question_type = '0' then entity_id else null end normal_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select province,city,org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_num,
case when question_type in ('11','12') then entity_id else null end suspicious_num,
case when question_type = '0' then entity_id else null end normal_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}') t
group by province,city,org_code,take_out_type

union all

select province,city,org_code,take_out_type,count(distinct warn_num) as warn_num,count(distinct suspicious_num) as suspicious_num,count(distinct normal_num) as normal_num,0 as registration_num,0 as entity_num from
(select province,city,substr(org_code,1,4) as org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_num,
case when question_type in ('11','12') then entity_id else null end suspicious_num,
case when question_type = '0' then entity_id else null end normal_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select province,city,substr(org_code,1,4)  as org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_num,
case when question_type in ('11','12') then entity_id else null end suspicious_num,
case when question_type = '0' then entity_id else null end normal_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}') t
group by province,city,org_code,take_out_type

union all

select province,'' as city,org_code,take_out_type,count(distinct warn_num) as warn_num,count(distinct suspicious_num) as suspicious_num,count(distinct normal_num) as normal_num,0 as registration_num,0 as entity_num from
(select province,substr(org_code,1,2)  as org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_num,
case when question_type in ('11','12') then entity_id else null end suspicious_num,
case when question_type = '0' then entity_id else null end normal_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select province,substr(org_code,1,2)  as org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_num,
case when question_type in ('11','12') then entity_id else null end suspicious_num,
case when question_type = '0' then entity_id else null end normal_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}') t
group by province,org_code,take_out_type

union all

select province,city,org_code,take_out_type,0 as warn_num,0 as suspicious_num,0 as normal_num,0 as registration_num,count(distinct entity_id) as entity_num from 
(select entity_id,province,city,org_code,take_out_type from takeout.tbl_ex_entity_question_info_snapshot where dt = '${yesterday}'
union all
select entity_id,province,city,org_code,'0' as take_out_type from takeout.tbl_ex_entity_question_info_snapshot where dt = '${yesterday}'
) t
group by province,city,org_code,take_out_type

union all

select province,'' as city,org_code,take_out_type,0 as warn_num,0 as suspicious_num,0 as normal_num,0 as registration_num,count(distinct entity_id) as entity_num from 
(select entity_id,province,substr(org_code,1,2) as org_code,take_out_type from takeout.tbl_ex_entity_question_info_snapshot where dt = '${yesterday}'
union all
select entity_id,province,substr(org_code,1,2) as org_code,'0' as take_out_type from takeout.tbl_ex_entity_question_info_snapshot where dt = '${yesterday}'
) t
group by province,org_code,take_out_type

union all

select province,city,org_code,take_out_type,0 as warn_num,0 as suspicious_num,0 as normal_num,0 as registration_num,count(distinct entity_id) as entity_num from 
(select entity_id,province,city,substr(org_code,1,4) as org_code,take_out_type from takeout.tbl_ex_entity_question_info_snapshot where dt = '${yesterday}'
union all
select entity_id,province,city,substr(org_code,1,4) as org_code,'0' as take_out_type from takeout.tbl_ex_entity_question_info_snapshot where dt = '${yesterday}'
) t
group by province,city,org_code,take_out_type

union all

select province,city,org_code,take_out_type,0 as warn_num,0 as suspicious_num,0 as normal_num,count(distinct entity_id) as registration_num,0 as entity_num from
(select province,city,org_code,take_out_type,shop_id as entity_id from takeout.tbl_ex_entity_register_snapshot where dt = '${yesterday}'
union all
select province,city,org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_register_snapshot where dt = '${yesterday}'
) t
group by province,city,org_code,take_out_type

union all

select province,'' as city,org_code,take_out_type,0 as warn_num,0 as suspicious_num,0 as normal_num,count(distinct entity_id) as registration_num,0 as entity_num from
(select province,substr(org_code,1,2) as org_code,take_out_type,shop_id as entity_id from takeout.tbl_ex_entity_register_snapshot where dt = '${yesterday}'
union all
select province,substr(org_code,1,2) as org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_register_snapshot where dt = '${yesterday}'
) t
group by province,org_code,take_out_type

union all

select province,city,org_code,take_out_type,0 as warn_num,0 as suspicious_num,0 as normal_num,count(distinct entity_id) as registration_num,0 as entity_num from
(select province,city,substr(org_code,1,4) as org_code,take_out_type,shop_id as entity_id from takeout.tbl_ex_entity_register_snapshot where dt = '${yesterday}'
union all
select province,city,substr(org_code,1,4) as org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_register_snapshot where dt = '${yesterday}'
) t
group by province,city,org_code,take_out_type
) t
group by province,city,org_code,take_out_type;"

hiveSqlToTakeoutMysql "${hive_sql}" "${yesterday}" "${mysql_table}" "date,province,city,orgcode,waimai_type,warn_num,suspicious_num,normal_num,registration_num,entity_num" "date"

echo "############### 计算waimai_statistics_total end #####################"

