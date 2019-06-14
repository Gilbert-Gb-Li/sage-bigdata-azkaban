#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1

############# 拆分礼物数据 start #############
hive_sql="SELECT record_time,trace_id,app_package_name,user_id,user_name,split(r1.message,'\002') AS message
FROM ias.tbl_ex_live_room_data_origin_orc as t
LATERAL VIEW explode(message_info) r1 AS message WHERE dt='${yesterday}' AND gift_info IS NOT NULL"

tmp_table=$(hiveSqlToTmpHive "${hive_sql}" "tmp_message")
############# 拆分礼物数据 end #############

############# 删除当天快照 start #############
deleteHdfsAndPartiton4Orc "live" "tbl_ex_message_info_snapshot" "${yesterday}"
############# 删除当天快照 end #############

################# 直播间消息快照表 start   ########################

insert_sql="INSERT INTO TABLE live.tbl_ex_message_info_snapshot PARTITION(dt='${yesterday}')
SELECT record_time,trace_id,app_package_name,user_id,user_name,
       message[0],message[1],message[2],'ias'
FROM ${tmp_table}"

executeHiveCommand "${insert_sql}"

dropHiveTable "${tmp_table}" "default"

############# 导入当天快照 end################

#################  直播间消息快照表 end  ########################
