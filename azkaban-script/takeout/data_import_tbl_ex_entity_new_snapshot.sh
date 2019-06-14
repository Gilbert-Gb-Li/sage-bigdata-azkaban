#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1

beforeyesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`

echo "=======================计算新增实体 start========================="
hive_sql="
insert into table takeout.tbl_ex_entity_new_snapshot PARTITION(dt='${yesterday}')
select t1.entity_id,t2.shop_id,t2.take_out_type,t2.province,t2.city,t2.org_code from
(select t1.entity_id from 
(select distinct entity_id from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}') as t1 
left join
(select distinct entity_id from takeout.tbl_ex_entity_info_snapshot where dt='${beforeyesterday}') as t2 
on t1.entity_id = t2.entity_id
where t2.entity_id is null) as t1 
left join
(select * from takeout.tbl_ex_entity_info_snapshot where dt='${yesterday}') as t2
on t1.entity_id=t2.entity_id;"

executeHiveCommand "${hive_sql}"
echo "=======================计算新增实体 end========================="