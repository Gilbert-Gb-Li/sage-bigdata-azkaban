#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2
echo "##############   导出orc表开始   ##################"
hive_sql1="insert into table ias.tbl_ex_live_online_anchor_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,user_id,user_name,page_num,current_page,extend_page_name,live_id
from ias.tbl_ex_live_online_anchor_data_origin
where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql1}"

echo "##############   导出 tbl_ex_live_online_anchor_data_origin_orc 结束  模板一   ##################"

hive_sql2="insert into table ias.tbl_ex_live_room_data_origin_orc  PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,user_id,user_name,is_first,is_last,current_income,income_unit,online_user_num,link,message_info,gift_info
from ias.tbl_ex_live_room_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql2}"

echo "##############   导出 tbl_ex_live_room_data_origin_orc 结束  模板二   ##################"

hive_sql3="insert into table ias.tbl_ex_live_user_info_data_origin_orc PARTITION(dt='${date}')
select record_time,trace_id,template_version,template_type,client_time,protocol_version,app_version,app_package_name,ias_client_version,ias_client_hsn_id,
job_id,task_id,location,mobile,ias_client_ip,user_id,user_name,sex,age,hometown,constellation,occupation,sign,user_level,anchor_level,identification,
follow_count,fans_count,is_live,income,cost,income_cost_unit,last_login_time,last_live_time,contact_list
from ias.tbl_ex_live_user_info_data_origin where dt='${date}' and hour='${hour}'"

executeHiveCommand "${hive_sql3}"

echo "##############   导出 tbl_ex_live_user_info_data_origin 结束  模板三   ##################"
