#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算可疑 start #####################"
yesterday=$1
mysql_table="waimai_statistics_suspicious"

hive_sql="
select '${yesterday}',province,city,org_code,take_out_type,
count(distinct info_error_num) + count(distinct cart_error_num) as suspicious_num,
count(distinct suspicious_entity_num) as suspicious_entity_num,
count(distinct info_error_num) as info_error_num,
count(distinct cart_error_num) as cart_error_num
from
(select dt,province,city,org_code,take_out_type,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,org_code,'0' as take_out_type,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,substr(org_code,1,4) as org_code,take_out_type,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,substr(org_code,1,4) as org_code,'0' as take_out_type,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,'' as city,substr(org_code,1,2) as org_code,take_out_type,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,'' as city,substr(org_code,1,2) as org_code,'0' as take_out_type,
case when question_type in ('11','12') then entity_id else null end suspicious_entity_num,
case when question_type = '12' then entity_id else null end info_error_num,
case when question_type = '11' then entity_id else null end cart_error_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'
) t
group by province,city,org_code,take_out_type;"

hiveSqlToTakeoutMysql "${hive_sql}" "${yesterday}" "${mysql_table}" "date,province,city,orgcode,waimai_type,suspicious_num,suspicious_entity_num,info_error_num,cart_error_num" "date"

echo "############### 计算可疑 end #####################"
