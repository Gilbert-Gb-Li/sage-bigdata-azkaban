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

echo "############ 主播用户详情全量快照表  start   #########"

for app in ${live_app_list};
  do

    echo "##################创建临时表     ###############"

    tmp_user_info="CREATE TEMPORARY TABLE default.tmp_user_info AS
    SELECT latest_record_time,biz_name,data_source,user_id,user_name,age,sex,family,sign,user_level,vip_level,constellation,
           hometown,occupation,follow_count,fans_count,income_app_coin,cost_app_coin,location,total_live_count,total_live_time,
           total_audience_count,total_gift_count,total_income,total_message_count,last_start_time
    FROM live_p2.tbl_ex_user_snapshot WHERE dt='${check_day}' AND hour='${check_hour}' AND app_id='${app}' and user_id is not null
    UNION ALL
    SELECT latest_record_time,biz_name,data_source,user_id,user_name,age,sex,family,sign,user_level,vip_level,constellation,
           hometown,occupation,follow_count,fans_count,income_app_coin,cost_app_coin,location,total_live_count,total_live_time,
           total_audience_count,total_gift_count,total_income,total_message_count,last_start_time
    FROM live_p2.tbl_ex_user_active_snapshot WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' and user_id is not null;
    "
    echo "${tmp_user_info}"
    echo "#################################"

    #echo "##################删除当天快照  start  ###############"
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_user_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo "#################删除当天快照  end  ###################"

    echo "############## 创建当天快照IAS start##################"
    hive_sql_2="INSERT INTO TABLE live_p2.tbl_ex_user_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')
    SELECT a.latest_record_time,a.biz_name,a.data_source,a.user_id,a.user_name,a.age,a.sex,a.family,a.sign,
           a.user_level,a.vip_level,a.constellation,a.hometown,a.occupation,a.follow_count,a.fans_count,
           a.income_app_coin,a.cost_app_coin,location,
           b.total_live_count, b.total_live_time,
           b.total_audience_count,b.total_gift_count,b.total_income,b.total_message_count,
           b.last_start_time
    FROM
    ( (SELECT latest_record_time,biz_name,data_source,user_id,user_name,age,sex,family,sign,user_level,vip_level,constellation,
              hometown,occupation,follow_count,fans_count,income_app_coin,cost_app_coin,location,total_live_count,total_live_time,
              total_audience_count,total_gift_count,total_income,total_message_count,last_start_time
       FROM (
          SELECT t.*, row_number() over (partition by t.biz_name,t.user_id order by t.latest_record_time desc) as num
          FROM default.tmp_user_info AS t
       ) AS r
       WHERE r.num=1
      ) as a
     LEFT JOIN
     (SELECT biz_name,user_id,
              sum(h.total_live_count) AS total_live_count,
              sum(h.total_live_time) as total_live_time,
              sum(h.total_audience_count) as total_audience_count,
              sum(h.total_gift_count) as total_gift_count,
              sum(h.total_income) as total_income,
              sum(h.total_message_count) as total_message_count,
              max(h.last_start_time) as last_start_time
       FROM default.tmp_user_info as h
       GROUP BY h.biz_name,h.user_id
      ) b
      ON a.user_id=b.user_id and a.biz_name=b.biz_name
    );"

    executeHiveCommand "${tmp_user_info} ${hive_sql_2}"

    echo "############## 创建当天快照IAS  end  ##################"

done

echo "############ 主播用户详情全量快照表  end #########"
