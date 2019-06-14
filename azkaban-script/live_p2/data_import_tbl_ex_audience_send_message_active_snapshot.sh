#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '############## 活跃发消息用户快照 start ###############'

for app in ${live_app_list};
  do

    #echo "##################删除当天快照  start   ###############"
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_audience_send_message_active_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo "#################删除当天快照  end   ###################"

    echo "##############创建当天活跃发消息用户快照  start ################"
    hive_sql="INSERT INTO TABLE live_p2.tbl_ex_audience_send_message_active_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT '${app}',data_source,audience_id,
           max(audience_name) AS audience_name,
           count(1) AS send_count,
           count(distinct user_id) AS send_anchor_count,
           count(distinct user_id,live_id) AS send_live_count
    FROM live_p2.tbl_ex_message_info_snapshot
    WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}'
    GROUP BY data_source,audience_id
    "

    executeHiveCommand "${hive_sql}"

    echo "##############创建当天活跃发消息用户快照  end ################"

done

echo '############## 活跃发消息用户快照  end ################'
