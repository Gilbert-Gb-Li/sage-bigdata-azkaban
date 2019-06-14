#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算活跃用户 start #####################"
yesterday=$1

echo "##################删除当天快照  start   ###############"
deleteHdfsAndPartiton4Orc "live" "tbl_ex_anchor_active_snapshot" "${yesterday}"
echo "#################删除当天快照  end   ###################"

echo "##############创建当天活跃用户快照IAS  start################"
hive_sql="INSERT INTO TABLE live.tbl_ex_anchor_active_snapshot PARTITION(dt='${yesterday}')
SELECT t1.app_package_name,t1.user_id,t2.user_name,t2.anchor_level,t2.sex,t2.age,t2.hometown,
       t2.constellation,t2.occupation,t2.sign,t2.identification,t2.follow_count,t2.fans_count,
       t2.income,t2.income_cost_unit,t2.last_login_time,t2.last_live_time,
       t2.contact_list,t1.data_source
FROM (
  SELECT user_id,app_package_name,'ias' AS data_source
  FROM ias.tbl_ex_live_online_anchor_data_origin_orc
  WHERE dt='${yesterday}'
  GROUP BY user_id,app_package_name
  UNION ALL
  SELECT user_id,web_site_name,'web' AS data_source
  FROM web.tbl_ex_live_online_anchor_data_origin_orc
  WHERE dt='${yesterday}'
  GROUP BY user_id,web_site_name
) AS t1
LEFT JOIN (
  SELECT * FROM live.tbl_ex_user_detail_info_snapshot WHERE dt='${yesterday}'
) AS t2
ON t1.user_id=t2.user_id AND t1.app_package_name=t2.biz_name AND t1.data_source=t2.data_source"
executeHiveCommand "${hive_sql}"
echo "##############创建当天活跃用户快照IAS  end################"
echo "############### 计算活跃用户 end #####################"
