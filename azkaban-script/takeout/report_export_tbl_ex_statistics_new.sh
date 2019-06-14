#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算waimai_statistics_new start #####################"
yesterday=$1
mysql_table="waimai_statistics_new"

hive_sql="
select dt,province,city,org_code,take_out_type,max(new_entity_num) as new_entity_num,max(new_authentication_num) as new_authentication_num from
(
select dt,province,city,org_code,take_out_type,count(distinct entity_id) as new_entity_num,0 as new_authentication_num from
(select dt,province,city,org_code,take_out_type,entity_id from takeout.tbl_ex_entity_new_snapshot where dt = '${yesterday}'
union all
select dt,province,city,org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_new_snapshot where dt = '${yesterday}') t
group by dt,province,city,org_code,take_out_type

union all

select dt,province,'' as city,org_code,take_out_type,count(distinct entity_id) as new_entity_num,0 as new_authentication_num from
(select dt,province,substr(org_code,1,2) as org_code,take_out_type,entity_id from takeout.tbl_ex_entity_new_snapshot where dt = '${yesterday}'
union all
select dt,province,substr(org_code,1,2) as org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_new_snapshot where dt = '${yesterday}') t
group by dt,province,org_code,take_out_type

union all

select dt,province,city,org_code,take_out_type,count(distinct entity_id) as new_entity_num,0 as new_authentication_num from
(select dt,province,city,substr(org_code,1,4) as org_code,take_out_type,entity_id from takeout.tbl_ex_entity_new_snapshot where dt = '${yesterday}'
union all
select dt,province,city,substr(org_code,1,4) as org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_new_snapshot where dt = '${yesterday}') t
group by dt,province,city,org_code,take_out_type


union all

select dt,province,city,org_code,take_out_type,0 as new_entity_num,count(distinct entity_id) as new_authentication_num from
(select dt,province,city,org_code,take_out_type,entity_id from takeout.tbl_ex_entity_register_new_snapshot where dt = '${yesterday}'
union all
select dt,province,city,org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_register_new_snapshot where dt = '${yesterday}') t
group by dt,province,city,org_code,take_out_type

union all

select dt,province,'' as city,org_code,take_out_type,0 as new_entity_num,count(distinct entity_id) as new_authentication_num from
(select dt,province,substr(org_code,1,2) as org_code,take_out_type,entity_id from takeout.tbl_ex_entity_register_new_snapshot where dt = '${yesterday}'
union all
select dt,province,substr(org_code,1,2) as org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_register_new_snapshot where dt = '${yesterday}') t
group by dt,province,org_code,take_out_type

union all

select dt,province,city,org_code,take_out_type,0 as new_entity_num,count(distinct entity_id) as new_authentication_num from
(select dt,province,city,substr(org_code,1,4) as org_code,take_out_type,entity_id from takeout.tbl_ex_entity_register_new_snapshot where dt = '${yesterday}'
union all
select dt,province,city,substr(org_code,1,4) as org_code,'0' as take_out_type,entity_id from takeout.tbl_ex_entity_register_new_snapshot where dt = '${yesterday}') t
group by dt,province,city,org_code,take_out_type
) t
group by dt,province,city,org_code,take_out_type;"

hiveSqlToTakeoutMysql "${hive_sql}" "${yesterday}" "${mysql_table}" "date,province,city,orgcode,waimai_type,new_entity_num,new_authentication_num" "date"

echo "############### 计算waimai_statistics_new end #####################"
