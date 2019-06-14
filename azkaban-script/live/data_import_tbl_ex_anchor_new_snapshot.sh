#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
##############新增用户快照 start###############

echo "##################删除当天快照  start   ###############"
deleteHdfsAndPartiton4Orc "live" "tbl_ex_anchor_new_snapshot" "${yesterday}"
echo "#################删除当天快照  end   ###################"

echo "##############创建当天新增用户快照  start################"
hive_sql="INSERT INTO TABLE live.tbl_ex_anchor_new_snapshot PARTITION(dt='${yesterday}')
SELECT t1.biz_name,t1.user_id,t1.user_name,t1.anchor_level,t1.sex,
       t1.age,t1.hometown,t1.constellation,t1.occupation,t1.sign,t1.identification,
       t1.follow_count,t1.fans_count,t1.income,t1.income_cost_unit,t1.last_login_time,
       t1.last_live_time,t1.contact_list,t1.data_source
FROM (
  SELECT a.* FROM (
    SELECT * FROM live.tbl_ex_anchor_active_snapshot WHERE dt='${yesterday}'
  ) AS a
  LEFT JOIN (
    SELECT user_id,biz_name,data_source
    FROM live.tbl_ex_anchor_new_snapshot WHERE dt<'${yesterday}'
  ) AS b
  ON a.user_id=b.user_id AND a.biz_name=b.biz_name AND a.data_source=b.data_source
  WHERE b.user_id is null
) AS t1"

executeHiveCommand "${hive_sql}"
##############创建当天新增用户快照  end################
##############新增用户快照 start###############
