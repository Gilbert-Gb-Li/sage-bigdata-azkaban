#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
############## 打赏用户活跃快照 start ###############

echo "##################删除当天快照  start   ###############"
deleteHdfsAndPartiton4Orc "live" "tbl_ex_send_gift_active_user_snapshot" "${yesterday}"
echo "#################删除当天快照  end   ###################"

echo "##############创建当天新增用户快照  start################"
hive_sql="INSERT INTO TABLE live.tbl_ex_send_gift_active_user_snapshot PARTITION(dt='${yesterday}')
SELECT biz_name,user_id,data_source,
       COUNT(1) AS send_count,
       SUM(CAST(gift_value AS bigint)) AS send_value
FROM live.tbl_ex_gift_info_snapshot
WHERE dt='${yesterday}'
GROUP BY biz_name,user_id,data_source"

executeHiveCommand "${hive_sql}"
############## 打赏用户活跃快照  end ################
