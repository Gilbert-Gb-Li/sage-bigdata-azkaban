#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '################# 直播间快照表 start   ########################'

for app in ${live_app_list};
  do
    #echo '############# 删除当天快照 start #############'
    #deleteLivePartiton4Orc "live_p2" "tbl_ex_user_active_snapshot" "${day}" "${hour}" "${app}" "${p2_location_live_snapshot}"
    #echo '############# 删除当天快照 end #############'

    echo '############# 导入当天快照 start ###########'

    insert_sql="INSERT INTO TABLE live_p2.tbl_ex_user_active_snapshot PARTITION(dt='${day}',hour='${hour}',app_id='${app}')

    SELECT a.latest_record_time,a.biz_name,'${ias_source}',b.user_id,b.user_name,b.age,b.sex,b.family,b.sign,
           b.user_level,b.vip_level,b.constellation,b.hometown,b.occupation,b.follow_count,b.fans_count,
           b.income AS income_app_coin,b.cost AS cost_app_coin,b.location,
           if(a.total_live_count is not null,a.total_live_count,0),
           if(a.total_live_time is not null,a.total_live_time,0),
           if(a.total_audience_count is not null,a.total_audience_count,0),
           if(a.total_gift_count is not null,a.total_gift_count,0),
           if(a.total_income is not null,a.total_income,0),
           if(a.total_message_count is not null,a.total_message_count,0),
           a.last_start_time
    FROM
    (
      (SELECT max(latest_record_time) AS latest_record_time, biz_name,user_id,
              max(start_time) AS last_start_time,
              count(DISTINCT live_id) AS total_live_count,
              sum(live_time) AS total_live_time,
              sum(audience_count) AS total_audience_count,
              sum(gift_count) AS total_gift_count, sum(income) AS total_income,
              sum(message_count) AS total_message_count
       FROM live_p2.tbl_ex_live_info_snapshot
       WHERE dt='${day}' AND hour='${hour}' AND app_id='${app}' and biz_name is not null and user_id is not null
       GROUP BY biz_name,user_id
      ) a
      LEFT JOIN
      (SELECT record_time,app_package_name, user_id,user_name,age,sex,family,sign,user_level,vip_level,
              constellation,hometown,occupation,follow_count,fans_count,income,cost,location
       FROM (
        SELECT t.*, row_number() over (partition by app_user_id order by record_time desc) num
        FROM (
          SELECT record_time,app_package_name,user_id,user_name,age,sex,family,sign,user_level,vip_level,
                 constellation,hometown,occupation,follow_count,fans_count,income,cost,location,
                 concat(app_package_name, user_id) AS app_user_id
          FROM ias_p2.tbl_ex_live_user_info_data_origin_orc
          WHERE dt='${day}' AND app_id='${app}' and app_package_name is not null and user_id is not null
         ) AS t
       ) AS r
       WHERE r.num=1
      ) b
      ON a.biz_name=b.app_package_name AND a.user_id=b.user_id
    )
    "

    executeHiveCommand "${insert_sql}"

    echo '############# 导入当天快照 end ###########'
done

echo '################# 直播间快照表 end  ########################'
