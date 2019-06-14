#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '############## 活跃观众信息快照 start ###############'

for app in ${live_app_list};
  do

    #echo "##################删除当天快照  start   ###############"
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_audience_active_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo "#################删除当天快照  end   ###################"

    echo "##############创建当天观众信息快照 start ################"
    hive_sql="INSERT INTO TABLE live_p2.tbl_ex_audience_active_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT if(a.biz_name IS NOT null,a.biz_name,b.biz_name) AS biz_name,
           if(a.data_source IS NOT null,a.data_source,b.data_source) AS data_source,
           if(a.audience_id IS NOT null,a.audience_id,b.audience_id) AS audience_id,
           if(a.audience_name IS NOT null,a.audience_name,b.audience_name) AS audience_name,
           if(a.send_count IS NOT null,a.send_count,0) AS send_gift_count,
           if(a.send_value IS NOT null,a.send_value,0) AS send_gift_value,
           if(a.send_anchor_count IS NOT null,a.send_anchor_count,0) AS send_gift_anchor_count,
           if(a.send_live_count IS NOT null,a.send_live_count,0) AS send_gift_live_count,
           if(b.send_count IS NOT null,b.send_count,0) AS send_message_count,
           if(b.send_anchor_count IS NOT null,b.send_anchor_count,0) AS send_message_anchor_count,
           if(b.send_live_count IS NOT null,b.send_live_count,0) AS send_message_live_count
    FROM
    (
      (SELECT * FROM live_p2.tbl_ex_audience_send_gift_active_snapshot WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}') a
      FULL JOIN
      (SELECT * FROM live_p2.tbl_ex_audience_send_message_active_snapshot WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}') b
      ON a.biz_name=b.biz_name AND a.audience_id=b.audience_id
    )
    "

    executeHiveCommand "${hive_sql}"

    echo "##############创建当天观众信息快照 end ################"

done

echo '############## 活跃观众信息快照  end ################'
