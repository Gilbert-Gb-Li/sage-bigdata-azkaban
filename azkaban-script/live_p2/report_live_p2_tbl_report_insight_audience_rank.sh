#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

echo "############### 行业洞察-观众排行 报表统计 start #####################"

mysql_table="tbl_live_p2_insight_audience_rank"

###report_insight_rank_section="1 7 30"

for section in ${report_insight_rank_section};
  do

    echo "############### section: ${section} start #####################"

    diff=`expr $section - 1`

    start_day=`date -d "-$diff day $day" +%Y-%m-%d`



    hive_sql_1="SELECT a.biz_name,a.data_source,a.audience_id,a.audience_name,
                      a.send_gift_count,
                      a.send_gift_value,
                      if (b.send_anchor_count IS NOT NULL, b.send_anchor_count, 0) AS send_gift_anchor_count,
                      if (b.send_live_count IS NOT NULL, b.send_live_count, 0) AS send_gift_live_count,
                      a.send_message_count,
                      if (c.send_anchor_count IS NOT NULL, c.send_anchor_count, 0) AS send_message_anchor_count,
                      if (c.send_live_count IS NOT NULL, c.send_live_count, 0) AS send_message_live_count
               FROM
               (SELECT biz_name,data_source,audience_id,max(audience_name) AS audience_name,
                       sum(send_gift_count) AS send_gift_count,
                       sum(send_gift_value) AS send_gift_value,
                       sum(send_message_count) AS send_message_count
                FROM live_p2.tbl_ex_audience_active_snapshot
                WHERE dt>='${start_day}' AND dt<='${day}'
                GROUP BY biz_name,data_source,audience_id
               ) a
               FULL JOIN
               (SELECT biz_name,data_source,audience_id,
                       count(distinct user_id) AS send_anchor_count,
                       count(distinct user_id,live_id) AS send_live_count
                FROM live_p2.tbl_ex_gift_info_snapshot
                WHERE dt>='${start_day}' AND dt<='${day}'
                GROUP BY biz_name,data_source,audience_id
               ) b
               ON a.biz_name=b.biz_name AND a.data_source=b.data_source AND a.audience_id=b.audience_id
               FULL JOIN
               (SELECT biz_name,data_source,audience_id,
                       count(distinct user_id) AS send_anchor_count,
                       count(distinct user_id,live_id) AS send_live_count
                FROM live_p2.tbl_ex_message_info_snapshot
                WHERE dt>='${start_day}' AND dt<='${day}'
                GROUP BY biz_name,data_source,audience_id
               ) c
               ON a.biz_name=c.biz_name AND a.data_source=c.data_source AND a.audience_id=c.audience_id
               "


    hive_sql2="
    SELECT concat('${start_day}','~','${day}'),
           r.send_gift_value AS rank_point,
           r.biz_name,r.data_source,r.audience_id,r.audience_name,
           r.send_gift_count,r.send_gift_value,r.send_gift_anchor_count,r.send_gift_live_count,
           r.send_message_count,r.send_message_anchor_count,r.send_message_live_count
    FROM
    (
       SELECT t.*, row_number() OVER (PARTITION BY biz_name,data_source ORDER BY send_gift_value DESC) num
       FROM (
           ${hive_sql_1}
       ) AS t
    ) AS r WHERE r.num<=${report_max_audience_count}
    "

    hiveSqlToMysql "${hive_sql2}" "${start_day}~${day}" "${mysql_table}" "dt_section,rank_point,biz_name,data_source,audience_id,audience_name,send_gift_count,send_gift_value,send_gift_anchor_count,send_gift_live_count,send_message_count,send_message_anchor_count,send_message_live_count" "dt_section"


    echo "############### section: ${section} end #####################"
done


echo "############### 行业洞察-观众排行 报表统计 end #####################"
