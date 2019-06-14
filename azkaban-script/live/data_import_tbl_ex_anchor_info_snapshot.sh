#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1
beforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
echo "############ 导入数据主播详情 start   #########"

echo "##################删除当天快照  start  ###############"
deleteHdfsAndPartiton4Orc "live" "tbl_ex_anchor_info_snapshot" "${yesterday}"
echo "#################删除当天快照  end  ###################"

hive_sql="INSERT INTO TABLE live.tbl_ex_anchor_info_snapshot PARTITION(dt='${yesterday}')
SELECT t1.user_id,t1.user_name,t1.biz_name,
       ((case when (t1.income ='-1' or t1.income is null) then 0 else t1.income end) - (case when t2.income ='-1' or t2.income is null then 0 else t2.income end)) AS income,
       case when t3.message_count ='-1' or t3.message_count is null then 0 else t3.message_count end,
       ((case when (t1.fans_count ='-1' or t1.fans_count is null) then 0 else t1.fans_count end) - (case when t2.fans_count ='-1' or t2.fans_count is null then 0 else t2.fans_count end)) AS fans_count,
       t1.data_source
FROM (
  SELECT biz_name,user_id,user_name,sign,fans_count,income,data_source
  FROM live.tbl_ex_user_detail_info_snapshot WHERE dt='${yesterday}'
) AS t1
LEFT JOIN (
  SELECT biz_name,user_id,fans_count,income,data_source
  FROM live.tbl_ex_user_detail_info_snapshot WHERE dt='${beforeYesterday}'
) AS t2
ON t1.biz_name=t2.biz_name AND t1.user_id=t2.user_id AND t1.data_source=t2.data_source
LEFT JOIN (
  SELECT biz_name,anchor_id,data_source,count(1) AS message_count
  FROM live.tbl_ex_message_info_snapshot WHERE dt='${yesterday}'
  GROUP BY biz_name,anchor_id,data_source) AS t3
ON t1.biz_name=t3.biz_name AND t1.user_id=t3.anchor_id AND t1.data_source=t3.data_source"

executeHiveCommand "${hive_sql}"
echo "############ 导入数据  end   #########"
