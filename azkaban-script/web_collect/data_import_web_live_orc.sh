#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2
echo "##############   导出orc表开始   ##################"
hive_sql1="INSERT INTO TABLE web.tbl_ex_live_online_anchor_data_origin_orc PARTITION(dt='${date}')
SELECT record_time,trace_id,template_version,template_type,client_time,protocol_version,
       web_site_id,web_site_name,web_spider_version,node_id,
       task_id,user_id,user_name,user_icon_url,online_num,live_room_id,live_room_name,
       live_room_url,live_room_image_url,category_id,category_name,category_url
FROM web.tbl_ex_live_online_anchor_data_origin
WHERE dt='${date}' AND hour='${hour}'"

executeHiveCommand "${hive_sql1}"

echo "############## 导出 tbl_ex_live_online_anchor_data_origin_orc 结束  模板一 ##################"

hive_sql2="INSERT INTO TABLE web.tbl_ex_live_room_data_origin_orc PARTITION(dt='${date}')
SELECT record_time,trace_id,template_version,template_type,client_time,protocol_version,
       web_site_id,web_site_name,web_spider_version,node_id,
       task_id,user_id,user_name,message_info,gift_info
FROM web.tbl_ex_live_room_data_origin
WHERE dt='${date}' AND hour='${hour}'"

executeHiveCommand "${hive_sql2}"

echo "##############   导出 tbl_ex_live_room_data_origin_orc 结束  模板二   ##################"

hive_sql3="INSERT INTO TABLE web.tbl_ex_live_user_info_data_origin_orc PARTITION(dt='${date}')
SELECT record_time,trace_id,template_version,template_type,client_time,protocol_version,
       web_site_id,web_site_name,web_spider_version,node_id,
       task_id,user_id,user_name,sex,age,sign,user_level,anchor_level,
       follow_count,fans_count,is_live
FROM web.tbl_ex_live_user_info_data_origin
WHERE dt='${date}' AND hour='${hour}'"

executeHiveCommand "${hive_sql3}"

echo "##############   导出 tbl_ex_live_user_info_data_origin_orc 结束  模板三   ##################"
