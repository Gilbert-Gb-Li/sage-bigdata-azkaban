#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '############## 新增打赏观众快照 start ###############'

for app in ${live_app_list};
  do

    #echo "##################删除当天快照  start   ###############"
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_audience_send_gift_new_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo "#################删除当天快照  end   ###################"

    echo "##############创建当天新增打赏观众快照  start ################"
    hive_sql="INSERT INTO TABLE live_p2.tbl_ex_audience_send_gift_new_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT a.biz_name,
          a.data_source,
          a.audience_id,
          a.audience_name,
          a.send_count,
          a.send_value,
          a.send_anchor_count,
          a.send_live_count
    FROM
    (
      (SELECT * FROM live_p2.tbl_ex_audience_send_gift_active_snapshot WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}') a
      LEFT JOIN
      (SELECT * FROM live_p2.tbl_ex_audience_send_gift_active_snapshot WHERE (dt<'${day}' OR (dt='${day}' AND hour<'${hour}')) AND app_id='${app}') b
      ON a.audience_id=b.audience_id
    ) WHERE b.audience_id IS null
    "

    executeHiveCommand "${hive_sql}"

    echo "##############创建当天新增打赏观众快照  end ################"

done

echo '############## 新增打赏观众快照 end ################'
