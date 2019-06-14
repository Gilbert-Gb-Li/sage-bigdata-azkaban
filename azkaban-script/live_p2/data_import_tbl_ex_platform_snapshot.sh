#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '############## 平台信息快照 start ###############'

echo "############## 创建平台信息快照  start ################"



hive_sql="INSERT INTO TABLE live_p2.tbl_ex_platform_snapshot
SELECT concat(unix_timestamp(),substr(cast(rand() as string),3,3)),
       '${day}' as dt,
       '${hour}' as hour,
       b.biz_name,
       b.data_source,
       sum(b.live_count),
       sum(b.live_time),
       sum(b.active_user_count),
       sum(b.new_active_user_count),
       sum(b.recv_gift_live_count),
       sum(b.recv_gift_user_count),
       sum(b.new_recv_gift_user_count),
       sum(b.recv_message_live_count),
       sum(b.recv_message_user_count),
       sum(b.new_recv_message_user_count),
       sum(b.income),
       sum(b.gift_count),
       sum(b.message_count),
       sum(b.audience_count),
       sum(b.send_gift_audience_count),
       sum(b.new_send_gift_audience_count),
       sum(b.send_message_audience_count),
       sum(b.new_send_message_audience_count),
       sum(b.active_audience_count),
       sum(b.violation_count)
FROM
( SELECT a1.biz_name,a1.data_source,
       cast(a1.live_count as bigint) as live_count,cast(a1.live_time as bigint) as live_time,cast(a1.active_user_count as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(a1.audience_count as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name,data_source,count(1) AS live_count,sum(live_time) AS live_time,count(DISTINCT user_id) AS active_user_count,
          sum(audience_count) AS audience_count
       FROM live_p2.tbl_ex_live_info_snapshot
       WHERE dt='${day}' AND hour='${hour}' AND is_live=1
       GROUP BY biz_name,data_source
  ) a1
  UNION ALL
  SELECT a2.biz_name,a2.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(a2.income as double) as income,cast(a2.gift_count as bigint) as gift_count,
       cast(a2.message_count as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name,data_source,
          sum(income) AS income,
          sum(gift_count) AS gift_count,
          sum(message_count) AS message_count
      FROM live_p2.tbl_ex_live_info_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a2
  UNION ALL
  SELECT a3.biz_name,a3.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(a3.new_active_user_count as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS new_active_user_count
      FROM live_p2.tbl_ex_user_new_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a3
  UNION ALL
  SELECT a4.biz_name,a4.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(a4.recv_gift_live_count as bigint) as recv_gift_live_count,
       cast(a4.recv_gift_user_count as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS recv_gift_live_count, count(DISTINCT user_id) AS recv_gift_user_count
      FROM live_p2.tbl_ex_live_info_snapshot
      WHERE dt='${day}' AND hour='${hour}' AND gift_count>0
      GROUP BY biz_name, data_source
  ) a4
  UNION ALL
  SELECT a5.biz_name,a5.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(a5.new_recv_gift_user_count as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS new_recv_gift_user_count
      FROM live_p2.tbl_ex_user_new_recv_gift_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a5
  UNION ALL
  SELECT a6.biz_name,a6.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(a6.recv_message_live_count as bigint) as recv_message_live_count,cast(a6.recv_message_user_count as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS recv_message_live_count, count(DISTINCT user_id) AS recv_message_user_count
      FROM live_p2.tbl_ex_live_info_snapshot
      WHERE dt='${day}' AND hour='${hour}' AND message_count>0
      GROUP BY biz_name, data_source
  ) a6
  UNION ALL
  SELECT a7.biz_name,a7.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(a7.new_recv_message_user_count as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS new_recv_message_user_count
      FROM live_p2.tbl_ex_user_new_recv_message_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a7
  UNION ALL
  SELECT a8.biz_name,a8.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(a8.send_gift_audience_count as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS send_gift_audience_count
      FROM live_p2.tbl_ex_audience_send_gift_active_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a8
  UNION ALL
  SELECT a9.biz_name,a9.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(a9.new_send_gift_audience_count as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS new_send_gift_audience_count
      FROM live_p2.tbl_ex_audience_send_gift_new_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a9
  UNION ALL
  SELECT a10.biz_name,a10.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(a10.send_message_audience_count as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS send_message_audience_count
      FROM live_p2.tbl_ex_audience_send_message_active_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a10
  UNION ALL
  SELECT a11.biz_name,a11.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(a11.new_send_message_audience_count as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS new_send_message_audience_count
      FROM live_p2.tbl_ex_audience_send_message_new_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a11
  UNION ALL
  SELECT a12.biz_name,a12.data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(a12.active_audience_count as bigint) as active_audience_count,cast(0 as bigint) as violation_count
  FROM(SELECT biz_name, data_source, count(1) AS active_audience_count
      FROM live_p2.tbl_ex_audience_active_snapshot
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY biz_name, data_source
  ) a12
  UNION ALL
  SELECT a13.biz_name,'${ias_source}' as data_source,
       cast(0 as bigint) as live_count,cast(0 as bigint) as live_time,cast(0 as bigint) as active_user_count,
       cast(0 as bigint) as new_active_user_count,cast(0 as bigint) as recv_gift_live_count,cast(0 as bigint) as recv_gift_user_count,
       cast(0 as bigint) as new_recv_gift_user_count,cast(0 as bigint) as recv_message_live_count,cast(0 as bigint) as recv_message_user_count,
       cast(0 as bigint) as new_recv_message_user_count,cast(0 as double) as income,cast(0 as bigint) as gift_count,
       cast(0 as bigint) as message_count,cast(0 as bigint) as audience_count,cast(0 as bigint) as send_gift_audience_count,
       cast(0 as bigint) as new_send_gift_audience_count,cast(0 as bigint) as send_message_audience_count,
       cast(0 as bigint) as new_send_message_audience_count,cast(0 as bigint) as active_audience_count,cast(a13.violation_count as bigint) as violation_count
  FROM(SELECT app_package_name AS biz_name, count(distinct order_id) AS violation_count
      FROM ias_p2.tbl_ex_live_record_data_origin_orc
      WHERE dt='${day}' AND hour='${hour}'
      GROUP BY app_package_name
  ) a13
) b
WHERE b.biz_name NOT IN (${sex_live_app_list})
GROUP BY b.biz_name,b.data_source
"

executeHiveCommand "${hive_sql}"

echo "############## 创建平台信息快照  end ################"

echo '############## 平台信息快照  end ################'
