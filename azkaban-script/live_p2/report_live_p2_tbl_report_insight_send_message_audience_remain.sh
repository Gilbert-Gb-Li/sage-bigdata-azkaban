#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
yesterday=`date -d "-1 day $day" +%Y-%m-%d`

echo "############### 行业洞察-发弹幕观众留存 报表统计 start #####################"

mysql_table="tbl_live_p2_insight_send_message_audience_remain"

for app in ${live_app_list};
  do
    execSqlOnMysql "INSERT INTO $mysql_table(dt,biz_name,data_source) VALUES('${yesterday}','${app}','${ias_source}')"
    update_sql="
      UPDATE $mysql_table a
      JOIN (
        SELECT dt,biz_name,data_source,new_send_message_audience_count
        FROM tbl_live_p2_insight_platform_daily_data
        WHERE dt='${yesterday}' AND biz_name='${app}'
      ) b
      ON a.dt=b.dt AND a.biz_name=b.biz_name AND a.data_source = b.data_source
      SET a.new_count=b.new_send_message_audience_count
    "

    echo ${update_sql}

    execSqlOnMysql "${update_sql}"
done

tmp_mysql_table="tmp_tbl_live_p2_insight_send_message_audience_remain"

create_tmp_mysql_table_sql="CREATE TABLE IF NOT EXISTS ${tmp_mysql_table} (dt VARCHAR(11),biz_name VARCHAR(100),data_source VARCHAR(20),remain_count INT)"
execSqlOnMysql "${create_tmp_mysql_table_sql}"

for remain_day in ${report_insight_remain_check_day};
  do

    echo "############### remain_day: ${remain_day} start #####################"

    new_day=`date -d "-$remain_day day $day" +%Y-%m-%d`

    column="day_${remain_day}"

    echo ${column} ${new_day}

    echo "##################创建临时表  start  ###############"

    hive_sql="
    SELECT '${new_day}', biz_name,data_source, COUNT(DISTINCT a.audience_id)
    FROM
    (
      (SELECT audience_id,biz_name,data_source FROM live_p2.tbl_ex_audience_send_message_active_snapshot WHERE dt='${new_day}') a
      LEFT JOIN
      (SELECT audience_id,biz_name,data_source FROM live_p2.tbl_ex_audience_active_snapshot WHERE dt='${day}') b
      ON a.audience_id=b.audience_id AND a.biz_name=b.biz_name AND a.data_source = b.data_source
    )
    WHERE b.audience_id IS NOT NULL
    GROUP BY a.biz_name,a.data_source
    "

    echo ${hive_sql}

    hiveSqlToMysql "${hive_sql}" "${day}" "${tmp_mysql_table}" "dt,biz_name,data_source,remain_count" "dt"

    echo "##################创建临时表  end  ###############"

    update_sql1="
      UPDATE $mysql_table a
      JOIN (
        SELECT dt,biz_name,data_source,remain_count
        FROM $tmp_mysql_table
        WHERE dt='${new_day}'
      ) b
      ON a.dt=b.dt AND a.biz_name=b.biz_name AND a.data_source=b.data_source
      SET a.${column}=b.remain_count
    "

    echo "${update_sql1}"

    execSqlOnMysql "${update_sql1}"

    update_sql2="UPDATE $mysql_table SET ${column}=0 WHERE dt='${new_day}' AND ${column}=-1"

    execSqlOnMysql "${update_sql2}"

    echo "############### remain_day: ${remain_day} end #####################"

done

execSqlOnMysql "DROP TABLE IF EXISTS ${tmp_mysql_table}"

echo "############### 行业洞察-发弹幕观众留存 报表统计 end #####################"
