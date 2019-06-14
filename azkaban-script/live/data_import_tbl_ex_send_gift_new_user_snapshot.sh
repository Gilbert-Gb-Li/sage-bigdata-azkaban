#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
############## 打赏用户新增快照 start ###############

echo "##################删除当天快照  start   ###############"
deleteHdfsAndPartiton4Orc "live" "tbl_ex_send_gift_new_user_snapshot" "${yesterday}"
echo "#################删除当天快照  end   ###################"

echo "##############创建当天新增用户快照  start################"
hive_sql="INSERT INTO TABLE live.tbl_ex_send_gift_new_user_snapshot PARTITION(dt='${yesterday}')
SELECT a.biz_name,a.user_id,a.data_source
FROM (
  SELECT biz_name,user_id,data_source
  FROM live.tbl_ex_send_gift_active_user_snapshot
  WHERE dt='${yesterday}'
) a
LEFT JOIN (
  SELECT biz_name,user_id,data_source
  FROM live.tbl_ex_send_gift_new_user_snapshot
  WHERE dt<'${yesterday}'
) b
ON a.biz_name=b.biz_name AND a.user_id=b.user_id AND a.data_source=b.data_source
WHERE b.user_id IS NULL"

executeHiveCommand "${hive_sql}"
############## 打赏用户新增快照  end ################
