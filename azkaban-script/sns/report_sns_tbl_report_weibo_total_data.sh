#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 微博累计报表统计 start #####################"
date=$1

mysql_table="tbl_report_weibo_total_data"

hive_sql="SELECT '${date}',MAX(a.send_user_count),MAX(a.comment_user_count),MAX(a.send_total_count),
       MAX(a.like_total_count),MAX(a.comment_total_count)
FROM (
  SELECT COUNT(DISTINCT user_id) AS send_user_count,0 AS comment_user_count,0 AS send_total_count,
         0 AS like_total_count,0 AS comment_total_count
  FROM sns.tbl_ex_weibo_user_action_snapshot
  WHERE dt <= '${date}' AND CAST(send_count AS BIGINT)>5
  UNION ALL
  SELECT 0 AS send_user_count,COUNT(DISTINCT user_id) AS comment_user_count,0 AS send_total_count,
         0 AS like_total_count,0 AS comment_total_count
  FROM sns.tbl_ex_weibo_user_action_snapshot
  WHERE dt <= '${date}' AND CAST(comment_count AS BIGINT)>1
  UNION ALL
  SELECT 0 AS send_user_count,0 AS comment_user_count,COUNT(DISTINCT article_id) AS send_total_count,
         0 AS like_total_count,0 AS comment_total_count
  FROM sns.tbl_ex_weibo_article_detail_snapshot WHERE dt='${date}'
  UNION ALL
  SELECT 0 AS send_user_count,0 AS comment_user_count,0 AS send_total_count,
         SUM(CAST(like_count AS BIGINT)) AS like_total_count,0 AS comment_total_count
  FROM sns.tbl_ex_weibo_article_detail_snapshot WHERE dt='${date}'
  UNION ALL
  SELECT 0 AS send_user_count,0 AS comment_user_count,0 AS send_total_count,
         0 AS like_total_count,SUM(CAST(comment_count AS BIGINT)) AS comment_total_count
  FROM sns.tbl_ex_weibo_article_detail_snapshot WHERE dt='${date}'
) a"

hiveSqlToMysql "${hive_sql}" "${date}" "${mysql_table}" "stat_date,send_user_count,comment_user_count,send_total_count,like_total_count,comment_total_count" "stat_date"

echo "############### 微博累计报表统计 end #####################"
