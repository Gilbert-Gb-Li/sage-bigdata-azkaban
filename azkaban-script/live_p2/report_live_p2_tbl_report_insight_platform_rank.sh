#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

echo "############### 行业洞察-行业排行 报表统计 start #####################"

mysql_table="tbl_live_p2_insight_platform_rank"

echo "################## 创建临时表  start  ###############"

hive_sql_tmp="
SELECT *
FROM
(
  SELECT a.*, row_number() OVER (PARTITION BY biz_name,data_source,dt ORDER BY record_time DESC) num
  FROM live_p2.tbl_ex_platform_daily_snapshot AS a
) r where r.num=1
"

tmp_table=$(hiveSqlToTmpHive "${hive_sql_tmp}" "tmp_platform_daily_unique")

echo "################## 创建临时表  end  ###############"


for section in ${report_insight_rank_section};
  do

    echo "############### section: ${section} start #####################"

    diff=`expr $section - 1`

    start_day=`date -d "-$diff day $day" +%Y-%m-%d`

    hive_sql="
      SELECT t.dt_section,
             log10(t.income+1) / log10(m.max_income + 1) * ${platform_weight_income} +
             log10(t.active_user_count+1) / log10(m.max_active_user_count + 1) * ${platform_weight_active_user_count} +
             log10(t.live_count+1) / log10(m.max_live_count + 1) * ${platform_weight_live_count} +
             log10(t.message_count+1) / log10(m.max_message_count + 1) * ${platform_weight_message_count},
             t.biz_name,
             t.data_source,
             t.live_count,
             t.active_user_count,
             t.new_active_user_count,
             t.recv_gift_live_count,
             t.recv_gift_user_count,
             t.new_recv_gift_user_count,
             t.recv_message_live_count,
             t.recv_message_user_count,
             t.new_recv_message_user_count,
             t.income,
             t.gift_count,
             t.message_count,
             t.audience_count,
             t.send_gift_audience_count,
             t.new_send_gift_audience_count,
             t.send_message_audience_count,
             t.new_send_message_audience_count,
             t.active_audience_count
      FROM
      (
        SELECT concat('${start_day}','~','${day}') AS dt_section,
               a.biz_name,
               a.data_source,
               b.live_count,
               a.live_time,
               b.active_user_count,
               a.new_active_user_count,
               c.recv_gift_live_count,
               c.recv_gift_user_count,
               a.new_recv_gift_user_count,
               d.recv_message_live_count,
               d.recv_message_user_count,
               a.new_recv_message_user_count,
               a.income,
               a.gift_count,
               a.message_count,
               a.audience_count,
               e.send_gift_audience_count,
               a.new_send_gift_audience_count,
               f.send_message_audience_count,
               a.new_send_message_audience_count,
               g.active_audience_count
        FROM
        (
          (SELECT biz_name, data_source,
                  sum(live_time) AS live_time,
                  sum(new_active_user_count) AS new_active_user_count,
                  sum(new_recv_gift_user_count) AS new_recv_gift_user_count,
                  sum(new_recv_message_user_count) AS new_recv_message_user_count,
                  sum(income) AS income,
                  sum(gift_count) AS gift_count,
                  sum(message_count) AS message_count,
                  sum(audience_count) AS audience_count,
                  sum(new_send_gift_audience_count) AS new_send_gift_audience_count,
                  sum(new_send_message_audience_count) AS new_send_message_audience_count
           FROM ${tmp_table}
           WHERE dt >='${start_day}' AND dt<='${day}'
           GROUP BY biz_name, data_source
           ) a
          FULL JOIN
          (SELECT biz_name, data_source,
                  count(DISTINCT live_id) AS live_count,
                  count(DISTINCT user_id) AS active_user_count
           FROM live_p2.tbl_ex_live_info_snapshot
           WHERE dt >='${start_day}' AND dt<='${day}'
           GROUP BY biz_name, data_source
           ) b
           ON a.biz_name=b.biz_name AND a.data_source=b.data_source
          FULL JOIN
          (SELECT biz_name, data_source,
                  count(DISTINCT live_id) AS recv_gift_live_count,
                  count(DISTINCT user_id) AS recv_gift_user_count
           FROM live_p2.tbl_ex_live_info_snapshot
           WHERE dt >='${start_day}' AND dt<='${day}' AND gift_count>0
           GROUP BY biz_name, data_source
           ) c
           ON a.biz_name=c.biz_name AND a.data_source=c.data_source
          FULL JOIN
          (SELECT biz_name, data_source,
                  count(DISTINCT live_id) AS recv_message_live_count,
                  count(DISTINCT user_id) AS recv_message_user_count
           FROM live_p2.tbl_ex_live_info_snapshot
           WHERE dt >='${start_day}' AND dt<='${day}' AND message_count>0
           GROUP BY biz_name, data_source
           ) d
           ON a.biz_name=d.biz_name AND a.data_source=d.data_source
          FULL JOIN
          (SELECT biz_name, data_source, count(DISTINCT audience_id) AS send_gift_audience_count
           FROM live_p2.tbl_ex_audience_send_gift_active_snapshot
           WHERE dt >='${start_day}' AND dt<='${day}'
           GROUP BY biz_name, data_source
           ) e
          ON a.biz_name=e.biz_name AND a.data_source=e.data_source
          FULL JOIN
          (SELECT biz_name, data_source, count(DISTINCT audience_id) AS send_message_audience_count
           FROM live_p2.tbl_ex_audience_send_message_active_snapshot
           WHERE dt >='${start_day}' AND dt<='${day}'
           GROUP BY biz_name, data_source
           ) f
          ON a.biz_name=f.biz_name AND a.data_source=f.data_source
          FULL JOIN
          (SELECT biz_name, data_source, count(DISTINCT audience_id) AS active_audience_count
           FROM live_p2.tbl_ex_audience_active_snapshot
           WHERE dt >='${start_day}' AND dt<='${day}'
           GROUP BY biz_name, data_source
           ) g
          ON a.biz_name=g.biz_name AND a.data_source=g.data_source
        )
      ) AS t
      LEFT JOIN
      (
        SELECT concat('${start_day}','~','${day}') AS dt_section,
               max(a.income) AS max_income,
               max(a.active_user_count) AS max_active_user_count,
               max(a.live_count) AS max_live_count,
               max(a.message_count) AS max_message_count
        FROM
        (
            SELECT sum(income) as income, sum(active_user_count) as active_user_count,
                   sum(live_count) as live_count, sum(message_count) as message_count
            FROM ${tmp_table}
            WHERE dt >='${start_day}' AND dt<='${day}'
            GROUP BY biz_name,data_source
          ) a
      ) AS m
      ON t.dt_section=m.dt_section
    "

    echo ${hive_sql}

    hiveSqlToMysql "${hive_sql}" "${start_day}~${day}" "${mysql_table}" "dt_section,rank_point,biz_name,data_source,live_count,live_time,active_user_count,new_active_user_count,recv_gift_live_count,recv_gift_user_count,new_recv_gift_user_count,recv_message_live_count,recv_message_user_count,new_recv_message_user_count,income,gift_count,message_count,audience_count,send_gift_audience_count,new_send_gift_audience_count,send_message_audience_count,new_send_message_audience_count,active_audience_count" "dt_section"

    echo "############### section: ${section} end #####################"

done

dropHiveTable "${tmp_table}" "default"

echo "############### 行业洞察-行业排行 报表统计 end #####################"
