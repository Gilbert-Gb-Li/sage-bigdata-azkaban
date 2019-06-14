#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算预警 start #####################"
yesterday=$1
mysql_table="waimai_statistics_warn"

hive_sql="
select '${yesterday}',province,city,org_code,take_out_type,
count(distinct warn_entity_num) as warn_entity_num,
count(distinct no_authinfo_num) as no_authinfo_num,
count(distinct no_operatephoto_num) as no_operatephoto_num,
count(distinct no_licensephoto_num) as no_licensephoto_num,
count(distinct license_error_num) as license_error_num,
count(distinct license_expire_error_num) as license_expire_error_num,
count(distinct license_overdue_error_num) as license_overdue_error_num,
0 as name_error_num,
0 as address_error_num,
0 as owner_error_num,
0 as phone_error_num,
count(distinct licence_multi_purpose_num) as licence_multi_purpose_num
from
(select dt,province,city,org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type = '8' then entity_id else null end no_authinfo_num,
case when question_type = '7' then entity_id else null end no_operatephoto_num,
case when question_type = '6' then entity_id else null end no_licensephoto_num,
case when question_type = '9' then entity_id else null end license_error_num,
case when question_type = '10' then entity_id else null end license_expire_error_num,
case when question_type = '13' then entity_id else null end license_overdue_error_num,
case when question_type = '14' then entity_id else null end licence_multi_purpose_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type = '8' then entity_id else null end no_authinfo_num,
case when question_type = '7' then entity_id else null end no_operatephoto_num,
case when question_type = '6' then entity_id else null end no_licensephoto_num,
case when question_type = '9' then entity_id else null end license_error_num,
case when question_type = '10' then entity_id else null end license_expire_error_num,
case when question_type = '13' then entity_id else null end license_overdue_error_num,
case when question_type = '14' then entity_id else null end licence_multi_purpose_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,substr(org_code,1,4) as org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type = '8' then entity_id else null end no_authinfo_num,
case when question_type = '7' then entity_id else null end no_operatephoto_num,
case when question_type = '6' then entity_id else null end no_licensephoto_num,
case when question_type = '9' then entity_id else null end license_error_num,
case when question_type = '10' then entity_id else null end license_expire_error_num,
case when question_type = '13' then entity_id else null end license_overdue_error_num,
case when question_type = '14' then entity_id else null end licence_multi_purpose_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,city,substr(org_code,1,4) as org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type = '8' then entity_id else null end no_authinfo_num,
case when question_type = '7' then entity_id else null end no_operatephoto_num,
case when question_type = '6' then entity_id else null end no_licensephoto_num,
case when question_type = '9' then entity_id else null end license_error_num,
case when question_type = '10' then entity_id else null end license_expire_error_num,
case when question_type = '13' then entity_id else null end license_overdue_error_num,
case when question_type = '14' then entity_id else null end licence_multi_purpose_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,'' as city,substr(org_code,1,2) as org_code,take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type = '8' then entity_id else null end no_authinfo_num,
case when question_type = '7' then entity_id else null end no_operatephoto_num,
case when question_type = '6' then entity_id else null end no_licensephoto_num,
case when question_type = '9' then entity_id else null end license_error_num,
case when question_type = '10' then entity_id else null end license_expire_error_num,
case when question_type = '13' then entity_id else null end license_overdue_error_num,
case when question_type = '14' then entity_id else null end licence_multi_purpose_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'

union all

select dt,province,'' as city,substr(org_code,1,2) as org_code,'0' as take_out_type,
case when question_type in ('8','7','6','9','10','13','14') then entity_id else null end warn_entity_num,
case when question_type = '8' then entity_id else null end no_authinfo_num,
case when question_type = '7' then entity_id else null end no_operatephoto_num,
case when question_type = '6' then entity_id else null end no_licensephoto_num,
case when question_type = '9' then entity_id else null end license_error_num,
case when question_type = '10' then entity_id else null end license_expire_error_num,
case when question_type = '13' then entity_id else null end license_overdue_error_num,
case when question_type = '14' then entity_id else null end licence_multi_purpose_num
from takeout.tbl_ex_entity_question_info_snapshot 
where dt = '${yesterday}'
) t
group by province,city,org_code,take_out_type;"

hiveSqlToTakeoutMysql "${hive_sql}" "${yesterday}" "${mysql_table}" "date,province,city,orgcode,waimai_type,warn_entity_num,no_authinfo_num,no_operatephoto_num,no_licensephoto_num,license_error_num,license_expire_error_num,license_expired_num,name_error_num,address_error_num,owner_error_num,phone_error_num,licence_multi_purpose_num" "date"

echo "############### 计算预警 end #####################"
