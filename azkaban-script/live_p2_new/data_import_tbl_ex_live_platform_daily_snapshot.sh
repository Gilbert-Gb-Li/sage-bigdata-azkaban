#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

echo "############## 创建当天平台信息快照  start ################"

hive_sql="INSERT INTO TABLE live_p2.tbl_ex_live_platform_daily_snapshot partition(dt='${day}')
SELECT concat(unix_timestamp(),substr(cast(rand() as string),3,3)),
       b.app_package_name,
       b.data_source,
       sum(b.live_count),
       sum(b.active_user_count),
       sum(b.recv_gift_live_count),
       sum(b.recv_gift_user_count),
       sum(b.recv_message_live_count),
       sum(b.recv_message_user_count),
       sum(b.income),
       sum(b.gift_count),
       sum(b.message_count),
       sum(b.audience_count),
       sum(b.send_gift_audience_count),
       sum(b.send_message_audience_count),
       sum(b.violation_count)
FROM
( SELECT a1.app_package_name,
         a1.data_source,
         cast(0 as bigint) as live_count,
         cast(0 as bigint) as active_user_count,
         cast(0 as bigint) as recv_gift_live_count,
         cast(0 as bigint) as recv_gift_user_count,
         cast(0 as bigint) as recv_message_live_count,
         cast(0 as bigint) as recv_message_user_count,
         cast(a1.income as double) as income,
         cast(a1.gift_count as bigint) as gift_count,
         cast(a1.message_count as bigint) as message_count,
         cast(0 as bigint) as audience_count,
         cast(0 as bigint) as send_gift_audience_count,
         cast(0 as bigint) as send_message_audience_count,
         cast(0 as bigint) as violation_count
  FROM(SELECT app_package_name,data_source,
          sum(income) AS income,
          sum(gift_count) AS gift_count,
          sum(message_count) AS message_count
      FROM live_p2.tbl_ex_live_info_snapshot_new
      WHERE dt='${day}'
      GROUP BY app_package_name,data_source
  ) a1
  UNION ALL
  SELECT a2.app_package_name,
         a2.data_source,
         cast(a2.live_count as bigint) as live_count,
         cast(a2.active_user_count as bigint) as active_user_count,
         cast(0 as bigint) as recv_gift_live_count,
         cast(0 as bigint) as recv_gift_user_count,
         cast(0 as bigint) as recv_message_live_count,
         cast(0 as bigint) as recv_message_user_count,
         cast(0 as double) as income,
         cast(0 as bigint) as gift_count,
         cast(0 as bigint) as message_count,
         cast(0 as bigint) as audience_count,
         cast(0 as bigint) as send_gift_audience_count,
         cast(0 as bigint) as send_message_audience_count,
         cast(0 as bigint) as violation_count
  FROM(SELECT app_package_name,data_source,
          count(DISTINCT live_id) AS live_count,
          count(DISTINCT user_id) AS active_user_count
      FROM live_p2.tbl_ex_live_info_snapshot_new
      WHERE dt='${day}' AND is_live=1
      GROUP BY app_package_name,data_source
  ) a2
  UNION ALL
  SELECT a3.app_package_name,
         a3.data_source,
         cast(0 as bigint) as live_count,
         cast(0 as bigint) as active_user_count,
         cast(a3.recv_gift_live_count as bigint) as recv_gift_live_count,
         cast(a3.recv_gift_user_count as bigint) as recv_gift_user_count,
         cast(0 as bigint) as recv_message_live_count,
         cast(0 as bigint) as recv_message_user_count,
         cast(0 as double) as income,
         cast(0 as bigint) as gift_count,
         cast(0 as bigint) as message_count,
         cast(0 as bigint) as audience_count,
         cast(0 as bigint) as send_gift_audience_count,
         cast(0 as bigint) as send_message_audience_count,
         cast(0 as bigint) as violation_count
  FROM(SELECT app_package_name, data_source,
          count(DISTINCT live_id) AS recv_gift_live_count,
          count(DISTINCT user_id) AS recv_gift_user_count
      FROM live_p2.tbl_ex_live_info_snapshot_new
      WHERE dt='${day}' AND gift_count>0
      GROUP BY app_package_name,data_source
  ) a3
  UNION ALL
  SELECT a4.app_package_name,
         a4.data_source,
         cast(0 as bigint) as live_count,
         cast(0 as bigint) as active_user_count,
         cast(0 as bigint) as recv_gift_live_count,
         cast(0 as bigint) as recv_gift_user_count,
         cast(a4.recv_message_live_count as bigint) as recv_message_live_count,
         cast(a4.recv_message_user_count as bigint) as recv_message_user_count,
         cast(0 as double) as income,
         cast(0 as bigint) as gift_count,
         cast(0 as bigint) as message_count,
         cast(0 as bigint) as audience_count,
         cast(0 as bigint) as send_gift_audience_count,
         cast(0 as bigint) as send_message_audience_count,
         cast(0 as bigint) as violation_count
  FROM(SELECT app_package_name, data_source,
          count(DISTINCT live_id) AS recv_message_live_count,
          count(DISTINCT user_id) AS recv_message_user_count
      FROM live_p2.tbl_ex_live_info_snapshot_new
      WHERE dt='${day}' AND message_count>0
      GROUP BY app_package_name,data_source
  ) a4
  UNION ALL
  SELECT a5.app_package_name,
         a5.data_source,
         cast(0 as bigint) as live_count,
         cast(0 as bigint) as active_user_count,
         cast(0 as bigint) as recv_gift_live_count,
         cast(0 as bigint) as recv_gift_user_count,
         cast(0 as bigint) as recv_message_live_count,
         cast(0 as bigint) as recv_message_user_count,
         cast(0 as double) as income,
         cast(0 as bigint) as gift_count,
         cast(0 as bigint) as message_count,
         cast(0 as bigint) as audience_count,
         cast(a5.send_gift_audience_count as bigint) as send_gift_audience_count,
         cast(0 as bigint) as send_message_audience_count,
         cast(0 as bigint) as violation_count
  FROM(SELECT app_package_name, data_source, count(DISTINCT audience_id) AS send_gift_audience_count
      FROM ias_p2.tbl_ex_live_gift_info_orc
      WHERE dt='${day}'
      GROUP BY app_package_name,data_source
  ) a5
  UNION ALL
  SELECT a6.app_package_name,
         a6.data_source,
         cast(0 as bigint) as live_count,
         cast(0 as bigint) as active_user_count,
         cast(0 as bigint) as recv_gift_live_count,
         cast(0 as bigint) as recv_gift_user_count,
         cast(0 as bigint) as recv_message_live_count,
         cast(0 as bigint) as recv_message_user_count,
         cast(0 as double) as income,
         cast(0 as bigint) as gift_count,
         cast(0 as bigint) as message_count,
         cast(0 as bigint) as audience_count,
         cast(0 as bigint) as send_gift_audience_count,
         cast(a6.send_message_audience_count as bigint) as send_message_audience_count,
         cast(0 as bigint) as violation_count
  FROM(SELECT app_package_name, data_source, count(DISTINCT audience_id) AS send_message_audience_count
      FROM ias_p2.tbl_ex_live_message_info_orc
      WHERE dt='${day}'
      GROUP BY app_package_name,data_source
  ) a6
  UNION ALL
  SELECT a8.app_package_name,
         a8.data_source,
         cast(0 as bigint) as live_count,
         cast(0 as bigint) as active_user_count,
         cast(0 as bigint) as recv_gift_live_count,
         cast(0 as bigint) as recv_gift_user_count,
         cast(0 as bigint) as recv_message_live_count,
         cast(0 as bigint) as recv_message_user_count,
         cast(0 as double) as income,
         cast(0 as bigint) as gift_count,
         cast(0 as bigint) as message_count,
         cast(audience_count as bigint) as audience_count,
         cast(0 as bigint) as send_gift_audience_count,
         cast(0 as bigint) as send_message_audience_count,
         cast(0 as bigint) as violation_count
  FROM(SELECT t.app_package_name,t.data_source,
             sum(audience_count) as audience_count
       FROM( SELECT app_package_name, data_source,
             max(audience_count) AS audience_count
             FROM live_p2.tbl_ex_live_info_snapshot_new
             WHERE dt='${day}' AND is_live=1
             GROUP BY app_package_name,data_source,user_id,live_id
       ) t
       GROUP BY app_package_name,data_source
  ) a8
  UNION ALL
  SELECT a13.app_package_name,
         '${ias_source}' as data_source,
         cast(0 as bigint) as live_count,
         cast(0 as bigint) as active_user_count,
         cast(0 as bigint) as recv_gift_live_count,
         cast(0 as bigint) as recv_gift_user_count,
         cast(0 as bigint) as recv_message_live_count,
         cast(0 as bigint) as recv_message_user_count,
         cast(0 as double) as income,
         cast(0 as bigint) as gift_count,
         cast(0 as bigint) as message_count,
         cast(0 as bigint) as audience_count,
         cast(0 as bigint) as send_gift_audience_count,
         cast(0 as bigint) as send_message_audience_count,
         cast(a13.violation_count as bigint) as violation_count
  FROM(SELECT app_package_name, count(distinct order_id) AS violation_count
        FROM ias_p2.tbl_ex_live_record_data_origin_orc
        WHERE dt>='${day}' and result_code=0 and (end_time-start_time)>=${live_record_video_length}
              and from_unixtime(cast(substr(cast(start_time as string),0,10) as bigint),'yyyy-MM-dd')='${day}'
        GROUP BY app_package_name
  ) a13
) b
GROUP BY b.app_package_name,b.data_source
"

executeHiveCommand "${hive_sql}"

echo "############## 创建当天平台信息快照  end ################"
