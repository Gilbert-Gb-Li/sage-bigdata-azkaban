#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

echo "############### 微博日报报表统计 start #####################"
date=$1

mysql_table="tbl_report_weibo_daily_data"

hive_sql="SELECT '${date}',MAX(a.send_count),MAX(a.send_user_count),MAX(a.comment_count),
       MAX(a.comment_user_count)
FROM (
  SELECT COUNT(DISTINCT a.article_id) AS send_count,0 AS send_user_count,
         0 AS comment_count,0 AS comment_user_count
  FROM (
    SELECT user_id,article_id
    FROM sns.tbl_ex_weibo_article_detail_snapshot
    WHERE dt='${date}'
  ) a
  JOIN sns.weibo_monitor_user b ON a.user_id=b.weibo_id
  UNION ALL
  SELECT 0 AS send_count,COUNT(DISTINCT a.user_id) AS send_user_count,
         0 AS comment_count,0 AS comment_user_count
  FROM (
    SELECT user_id
    FROM sns.tbl_ex_weibo_user_action_snapshot
    WHERE dt='${date}' AND CAST(send_count AS BIGINT) > 1
  ) a
  JOIN sns.weibo_monitor_user b
  ON a.user_id=b.weibo_id
  UNION ALL
  SELECT 0 AS send_count,0 AS send_user_count,
         COUNT(DISTINCT a.comment_id) AS comment_count,0 AS comment_user_count
  FROM (
    SELECT user_id,comment_id
    FROM ias.tbl_ex_weibo_comment_topic_data_origin_orc
    WHERE dt='${date}'
    GROUP BY user_id,comment_id
  ) a
  JOIN sns.weibo_monitor_user b
  ON a.user_id=b.weibo_id
  UNION ALL
  SELECT 0 AS send_count,0 AS send_user_count,
         0 AS comment_count,COUNT(DISTINCT a.user_id) AS comment_user_count
  FROM (
    SELECT user_id
    FROM sns.tbl_ex_weibo_user_action_snapshot
    WHERE dt='${date}' AND CAST(comment_count AS BIGINT) > 1
  ) a
  JOIN sns.weibo_monitor_user b
  ON a.user_id=b.weibo_id
) a"

hiveSqlToMysql "${hive_sql}" "${date}" "${mysql_table}" "stat_date,send_count,send_user_count,comment_count,comment_user_count" "stat_date"

echo "############### 微博日报报表统计 end #####################"
