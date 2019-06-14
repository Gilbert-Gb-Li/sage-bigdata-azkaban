#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1

################# 直播间礼物快照表 start   ########################

############# 拆分礼物数据 start #############
hive_sql="SELECT record_time,trace_id,app_package_name,user_id,user_name,split(r1.gift,'\002') AS gift
FROM ias.tbl_ex_live_room_data_origin_orc AS t
LATERAL VIEW explode(gift_info) r1 AS gift WHERE dt='${yesterday}' AND gift_info IS NOT NULL"

tmp_table=$(hiveSqlToTmpHive "${hive_sql}" "tmp_gift")
############# 拆分礼物数据 end #############

############# 删除当天快照 start #############
deleteHdfsAndPartiton4Orc "live" "tbl_ex_gift_info_snapshot" "${yesterday}"
############# 删除当天快照 end #############

############# 导入当天快照 start ###########
insert_sql="INSERT INTO TABLE live.tbl_ex_gift_info_snapshot PARTITION(dt='${yesterday}')
SELECT record_time,trace_id,app_package_name,user_id,user_name,
       gift[0],gift[1],gift[2],gift[3],gift[4],gift[5],gift[6],'ias'
FROM ${tmp_table}"

executeHiveCommand "${insert_sql}"

dropHiveTable "${tmp_table}" "default"

############# 导入当天快照 end################


################# 直播间礼物快照表 end  ########################
