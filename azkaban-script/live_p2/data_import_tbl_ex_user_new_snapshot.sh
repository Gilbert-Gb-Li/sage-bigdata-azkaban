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

echo '################# 新增直播用户快照表 start   ########################'

for app in ${live_app_list};
  do

    #echo '############# 删除当天快照 start #############'
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_user_new_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo '############# 删除当天快照 end #############'

    echo '############# 导入当天快照 start ###########'

    insert_sql="INSERT INTO TABLE live_p2.tbl_ex_user_new_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT a.latest_record_time,
           a.biz_name,
           a.data_source,
           a.user_id,
           a.user_name,
           a.age,
           a.sex,
           a.family,
           a.sign,
           a.user_level,
           a.vip_level,
           a.constellation,
           a.hometown,
           a.occupation,
           a.follow_count,
           a.fans_count,
           a.income_app_coin,
           a.cost_app_coin,
           a.location,
           a.total_live_count,
           a.total_live_time,
           a.total_audience_count,
           a.total_gift_count,
           a.total_income,
           a.total_message_count,
           a.last_start_time
    FROM
    (
      (SELECT * FROM live_p2.tbl_ex_user_active_snapshot WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' and user_id is not null ) a
      LEFT JOIN
      (SELECT * FROM live_p2.tbl_ex_user_snapshot WHERE dt='${check_day}' AND hour='${check_hour}' AND app_id='${app}' and user_id is not null ) b
      ON a.user_id=b.user_id
    ) WHERE b.user_id is null
    "

    executeHiveCommand "${insert_sql}"

    echo '############# 导入当天快照 end ###########'

done

echo '################# 新增直播用户 end  ########################'
