#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1


echo "############### 行业洞察-用户排行 报表统计 start #####################"

mysql_table="tbl_live_p2_insight_user_rank"

###report_insight_rank_section="1 7 30"

for section in ${report_insight_rank_section};
  do

    echo "############### section: ${section} start #####################"

    diff=`expr $section - 1`

    start_day=`date -d "-$diff day $day" +%Y-%m-%d`

    #echo "##################创建临时表 1  start  ###############"

    hive_sql1="CREATE TEMPORARY TABLE default.tmp_user_rank_1 AS
      SELECT concat('${start_day}','~','${day}') as dt_section,
             a.biz_name,a.data_source,a.user_id,
             b.total_live_count,
             a.total_live_time,
             a.total_audience_count,
             a.total_gift_count,
             a.total_income,
             a.total_message_count,
             a.fans_count,
             a.last_start_time
      FROM
        (SELECT biz_name,data_source,user_id,
               sum(total_live_time) AS total_live_time,
               sum(total_audience_count) AS total_audience_count,
               sum(total_gift_count) AS total_gift_count,
               sum(total_income) AS total_income,
               sum(total_message_count) AS total_message_count,
               avg(fans_count) AS fans_count,
               max(last_start_time) AS last_start_time
        FROM live_p2.tbl_ex_user_active_snapshot
        WHERE dt>='${start_day}' AND dt<='${day}'
        GROUP BY biz_name,data_source,user_id
        ) a
        LEFT JOIN
        (SELECT biz_name,data_source,user_id,count(DISTINCT live_id) AS total_live_count
          FROM live_p2.tbl_ex_live_info_snapshot
          WHERE dt>='${start_day}' AND dt<='${day}'
          GROUP BY biz_name,data_source,user_id
        ) b
        ON a.biz_name=b.biz_name AND a.data_source=b.data_source AND a.user_id=b.user_id;
    "

    echo "${hive_sql1}"
    #tmp_table1=$(hiveSqlToTmpHive "${hive_sql1}" "tmp_user_rank_1")

    #echo "##################创建临时表 1  end  ###############"

    echo "##################创建临时表 2  start  ###############"

    hive_sql2="CREATE TABLE default.tmp_user_rank_2 AS
    SELECT *
    FROM
    (
      SELECT c.*, row_number() OVER (PARTITION BY biz_name,data_source ORDER BY rank_point DESC) num
      FROM
      (
        SELECT a.*,
               (log10(a.total_income+1) / log10(b.max_total_income + 1) * ${user_weight_income} +
               log10(a.total_message_count+1) / log10(b.max_total_message_count + 1) * ${user_weight_message_count} +
               log10(a.total_live_count+1) / log10(b.max_total_live_count + 1) * ${user_weight_live_count} +
               log10(a.fans_count+1) / log10(max_fans_count + 1) * ${user_weight_fans_count}) AS rank_point
        FROM default.tmp_user_rank_1 AS a
        LEFT JOIN
        (
          SELECT concat('${start_day}','~','${day}') as dt_section,
                 max(total_live_count) AS max_total_live_count,
                 max(total_audience_count) AS max_total_audience_count,
                 max(total_gift_count) AS max_total_gift_count,
                 max(total_income) AS max_total_income,
                 max(total_message_count) AS max_total_message_count,
                 max(fans_count) AS max_fans_count
          FROM default.tmp_user_rank_1
        ) b
        ON a.dt_section=b.dt_section
      ) c
    ) d WHERE d.num<=${report_max_user_rank_count};
    "
    echo "${hive_sql2}"
    #tmp_table2=$(hiveSqlToTmpHive "${hive_sql2}" "tmp_user_rank_2")

    executeHiveCommand "${hive_sql1} ${hive_sql2}"

    echo "##################创建临时表 2  end  ###############"

    hive_sql3="
    SELECT a.dt_section,
           a.rank_point,
           a.biz_name,
           a.data_source,
           a.user_id,
           b.user_name,
           b.age,
           b.sex,
           b.family,
           b.sign,
           b.user_level,
           b.vip_level,
           b.constellation,
           b.hometown,
           b.occupation,
           b.follow_count,
           b.fans_count,
           b.income_app_coin,
           b.cost_app_coin,
           b.location,
           a.total_live_count,
           a.total_live_time,
           a.total_audience_count,
           a.total_gift_count,
           a.total_income,
           a.total_message_count,
           b.last_start_time
    FROM default.tmp_user_rank_2 AS a
    LEFT JOIN
    (SELECT *
     FROM live_p2.tbl_ex_user_snapshot
     WHERE dt='${day}' AND hour='23'
    ) b
    ON a.biz_name=b.biz_name AND a.data_source=b.data_source AND a.user_id=b.user_id
    "
    hiveSqlToMysql "${hive_sql3}" "${start_day}~${day}" "${mysql_table}" "dt_section,rank_point,biz_name,data_source,user_id,user_name,age,sex,family,sign,user_level,vip_level,constellation,hometown,occupation,follow_count,fans_count,income_app_coin,cost_app_coin,location,total_live_count,total_live_time,total_audience_count,total_gift_count,total_income,total_message_count,last_start_time" "dt_section"

    dropHiveTable "tmp_user_rank_2" "default"

    echo "############### section: ${section} end #####################"
done

echo "############### 行业洞察-用户排行 报表统计 end #####################"
