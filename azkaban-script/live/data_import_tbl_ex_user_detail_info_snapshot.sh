#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
beforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
echo "############ 用户详情快照表  start   #########"

echo "##################删除当天快照  start  ###############"
deleteHdfsAndPartiton4Orc "live" "tbl_ex_user_detail_info_snapshot" "${yesterday}"
echo "#################删除当天快照  end  ###################"

echo "############## 创建当天快照IAS start##################"
hive_sql="INSERT INTO TABLE live.tbl_ex_user_detail_info_snapshot PARTITION(dt='${yesterday}')
SELECT record_time,trace_id,biz_name,user_id,user_name,sex,age,hometown,constellation,
       occupation,sign,user_level,anchor_level,identification,follow_count,fans_count,is_live,
       income,cost,income_cost_unit,last_login_time,last_live_time,contact_list,data_source
FROM (
  SELECT t.*, row_number() over (partition by user_id order by record_time desc) num
  FROM (
    SELECT record_time,trace_id,biz_name,user_id,user_name,sex,age,hometown,constellation,
          occupation,sign,user_level,anchor_level,identification,follow_count,fans_count,is_live,
          income,cost,income_cost_unit,last_login_time,last_live_time,contact_list,data_source
    FROM live.tbl_ex_user_detail_info_snapshot WHERE dt='${beforeYesterday}'
    UNION ALL
    SELECT record_time,trace_id,app_package_name,user_id,user_name,sex,age,hometown,constellation,
           occupation,sign,user_level,anchor_level,identification,follow_count,fans_count,is_live,
           income,cost,income_cost_unit,last_login_time,last_live_time,contact_list,'ias' AS data_source
    FROM ias.tbl_ex_live_user_info_data_origin_orc WHERE dt='${yesterday}'
    UNION ALL
    SELECT record_time,trace_id,web_site_name,user_id,user_name,sex,age,'','',
           '',sign,user_level,anchor_level,'',follow_count,fans_count,is_live,
           '0','0','','','','','web' AS data_source
    FROM web.tbl_ex_live_user_info_data_origin_orc WHERE dt='${yesterday}'
  ) AS t
) AS r WHERE r.num=1"
executeHiveCommand "${hive_sql}"
echo "############## 创建当天快照IAS  end  ##################"
echo "############ 用户详情快照表  end #########"
