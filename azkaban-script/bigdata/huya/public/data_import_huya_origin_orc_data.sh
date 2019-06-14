#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

#########################################
#       导出live_id_list数据到orc        #
#########################################
hive_sql1="insert into table bigdata.huya_live_id_list_origin_orc PARTITION(dt='${date}')
select cloud_service_id,resource_key,data_type,schema,data_source,client_time,spider_version,app_version,app_package_name,container_id,data_generate_time,record_time,user_id,user_name,live_id,live_desc,search_id,user_image,share_url,location,current_page,target_id
from bigdata.huya_live_id_list_origin
where dt = '${date}';"

executeHiveCommand "${hive_sql1}"

#########################################
#      导出live_user_info数据到orc       #
#########################################

hive_sql2="insert into table bigdata.huya_live_user_info_origin_orc PARTITION(dt='${date}')
select cloud_service_id,resource_key,data_type,schema,data_source,client_time,spider_version,app_version,app_package_name,container_id,data_generate_time,record_time,user_id,user_name,user_notice,user_age,user_sign,user_location,province,city,user_level,user_love_channel,user_subscribe_num,user_fans_num,online_num,favor_num,room_id,is_live,live_duration
from bigdata.huya_live_user_info_origin
where dt = '${date}';"

executeHiveCommand "${hive_sql2}"

#########################################
#        导出live_danmu数据到orc         #
#########################################


hive_sql3="insert into table bigdata.huya_live_danmu_origin_orc PARTITION(dt='${date}')
select cloud_service_id,resource_key,data_type,schema,data_source,client_time,spider_version,app_version,app_package_name,container_id,data_generate_time,record_time,audience_id,audience_name,live_user_id,content,gift_num,gift_dribble,noble_classify,receiving_user,noble_num,for_name,for_id,for_room_id,is_this,danmu_type,u,t,v,gift_id,case when gift_md5 = '' or gift_md5 is null then null else gift_md5 end gift_md5,parent_resource_key,task_data_id,flow_id,source,guard_num_open,guard_num 
from bigdata.huya_live_danmu_origin 
where dt='${date}';"

executeHiveCommand "${hive_sql3}"

#########################################
#         导出live_gift数据到orc         #
#########################################

hive_sql4="insert into table bigdata.huya_live_gift_origin_orc PARTITION(dt='${date}')
select cloud_service_id,resource_key,data_type,schema,data_source,client_time,spider_version,app_version,app_package_name,container_id,data_generate_time,record_time,user_id,gift_id,gift_name,gift_gold,gift_icon,gift_md5 
from bigdata.huya_live_gift_origin 
where dt='${date}';"

executeHiveCommand "${hive_sql4}"

###################################################
#         导出live_week_rank_list数据到orc         #
###################################################

hive_sql4="insert into table bigdata.huya_live_week_rank_list_origin_orc PARTITION(dt='${date}')
select cloud_service_id,resource_key,data_type,schema,data_source,client_time,spider_version,app_version,app_package_name,container_id,data_generate_time,record_time,viewer_nobel_level,viewer_id,viewer_contribute,viewer_level,viewer_name,live_user_id
from bigdata.huya_live_week_rank_list_origin 
where dt='${date}';"

executeHiveCommand "${hive_sql4}"