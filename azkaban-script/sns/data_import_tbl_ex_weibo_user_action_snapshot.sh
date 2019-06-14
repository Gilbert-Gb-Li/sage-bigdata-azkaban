#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 计算用户微博动作统计 start #####################"
yesterday=$1

echo "##################删除当天快照  start   ###############"
deleteHdfsAndPartiton4Orc "sns" "tbl_ex_weibo_user_action_snapshot" "${yesterday}"
echo "#################删除当天快照  end   ###################"

hive_sql="INSERT INTO TABLE sns.tbl_ex_weibo_user_action_snapshot PARTITION(dt='${yesterday}')
SELECT a.user_id,MAX(send_count),MAX(comment_count),MAX(forward_count)
FROM (
  SELECT user_id,count(DISTINCT article_id) AS send_count,0 AS comment_count,0 AS forward_count
  FROM ias.tbl_ex_weibo_article_topic_data_origin_orc
  WHERE dt='${yesterday}' GROUP BY user_id
  UNION ALL
  SELECT user_id,0 AS send_count,count(DISTINCT comment_id) AS comment_count,0 AS forward_count
  FROM ias.tbl_ex_weibo_comment_topic_data_origin_orc
  WHERE dt='${yesterday}' GROUP BY user_id
  UNION ALL
  SELECT user_id,0 AS send_count,0 AS comment_count,count(DISTINCT forward_id) AS forward_count
  FROM ias.tbl_ex_weibo_forward_topic_data_origin_orc
  WHERE dt='${yesterday}' GROUP BY user_id
) a
GROUP BY user_id"

executeHiveCommand "${hive_sql}"

echo "############### 计算用户微博动作统计 end #####################"
