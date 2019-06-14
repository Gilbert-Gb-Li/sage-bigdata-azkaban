#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2


if [ "$hour" == "00" ]; then
    check_hour=23
    check_day=`date -d "-1 day $day" +%Y-%m-%d`
else
    hour_tmp=`expr $hour - 1`
    if [ ${#hour_tmp} == "1" ]; then
       check_hour="0"${hour_tmp}
    else
       check_hour=`expr $hour - 1`
    fi
    check_day=$day
fi

echo "############ " $day $hour $check_day $check_hour "#########"

echo '############## 全量观众信息快照 start ###############'

for app in ${live_app_list};
  do

    #echo "##################删除当天快照  start   ###############"
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_audience_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo "#################删除当天快照  end   ###################"

    hive_sql="INSERT INTO TABLE live_p2.tbl_ex_audience_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT '${app}','${ias_source}',audience_id,max(audience_name),
           sum(send_gift_count),sum(send_gift_value),sum(send_gift_anchor_count),sum(send_gift_live_count),
           sum(send_message_count),sum(send_message_anchor_count),sum(send_message_live_count)
    FROM
    (
      SELECT *
        FROM live_p2.tbl_ex_audience_active_snapshot
        WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}'
      UNION ALL
      SELECT *
        FROM live_p2.tbl_ex_audience_snapshot
        WHERE dt='${check_day}' AND hour='${check_hour}' AND app_id='${app}'
    ) a GROUP BY audience_id
    "

    executeHiveCommand "${hive_sql}"

done

echo '############## 全量观众信息快照  end ################'
