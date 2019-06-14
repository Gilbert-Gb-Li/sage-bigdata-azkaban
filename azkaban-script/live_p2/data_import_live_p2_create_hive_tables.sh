#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

####该脚本中的表结构在 hive项目中已有

echo "################# 创建 hive DB:live_p2 start  ########################"
executeHiveCommand "CREATE DATABASE IF NOT EXISTS live_p2"
echo "################# 创建 hive DB:live_p2 end  ########################"

echo "################# 创建 hive live_p2 tables start  ########################"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_gift_info_snapshot(
  record_time BIGINT,
  trace_id STRING,
  biz_name STRING,
  data_source STRING,
  data_generate_time BIGINT,
  search_id STRING,
  user_id STRING,
  live_id STRING,
  audience_id STRING,
  audience_name STRING,
  gift_id STRING,
  gift_type STRING,
  gift_name STRING,
  gift_image_url STRING,
  gift_count SMALLINT,
  gift_content STRING,
  gift_unit_price STRING,
  type STRING,
  gift_type_id STRING,
  gift_unit_val DOUBLE,
  gift_val DOUBLE
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_gift_info_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_message_info_snapshot(
  record_time BIGINT,
  trace_id STRING,
  biz_name STRING,
  data_source STRING,
  data_generate_time BIGINT,
  search_id STRING,
  user_id STRING,
  live_id STRING,
  audience_id STRING,
  audience_name STRING,
  content STRING,
  type STRING
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_message_info_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_live_info_snapshot(
  latest_record_time BIGINT,
  biz_name STRING,
  data_source STRING,
  search_id STRING,
  user_id STRING,
  live_id STRING,
  start_time BIGINT,
  end_time BIGINT,
  live_time INT,
  audience_count INT,
  gift_count INT,
  income DOUBLE,
  message_count INT,
  is_live INT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_live_info_snapshot'
"


executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_user_snapshot(
  latest_record_time BIGINT,
  biz_name STRING,
  data_source STRING,
  user_id STRING,
  user_name STRING,
  age SMALLINT,
  sex TINYINT,
  family STRING,
  sign STRING,
  user_level SMALLINT,
  vip_level SMALLINT,
  constellation STRING,
  hometown STRING,
  occupation STRING,
  follow_count INT,
  fans_count INT,
  income_app_coin STRING,
  cost_app_coin STRING,
  location STRING,
  total_live_count INT,
  total_live_time BIGINT,
  total_audience_count BIGINT,
  total_gift_count INT,
  total_income DOUBLE,
  total_message_count INT,
  last_start_time BIGINT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_user_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_user_active_snapshot(
  latest_record_time BIGINT,
  biz_name STRING,
  data_source STRING,
  user_id STRING,
  user_name STRING,
  age SMALLINT,
  sex TINYINT,
  family STRING,
  sign STRING,
  user_level SMALLINT,
  vip_level SMALLINT,
  constellation STRING,
  hometown STRING,
  occupation STRING,
  follow_count INT,
  fans_count INT,
  income_app_coin STRING,
  cost_app_coin STRING,
  location STRING,
  total_live_count INT,
  total_live_time BIGINT,
  total_audience_count BIGINT,
  total_gift_count INT,
  total_income DOUBLE,
  total_message_count INT,
  last_start_time BIGINT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_user_active_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_user_new_snapshot(
  latest_record_time BIGINT,
  biz_name STRING,
  data_source STRING,
  user_id STRING,
  user_name STRING,
  age SMALLINT,
  sex TINYINT,
  family STRING,
  sign STRING,
  user_level SMALLINT,
  vip_level SMALLINT,
  constellation STRING,
  hometown STRING,
  occupation STRING,
  follow_count INT,
  fans_count INT,
  income_app_coin STRING,
  cost_app_coin STRING,
  location STRING,
  total_live_count INT,
  total_live_time BIGINT,
  total_audience_count BIGINT,
  total_gift_count INT,
  total_income DOUBLE,
  total_message_count INT,
  last_start_time BIGINT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_user_new_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_user_new_recv_gift_snapshot(
  latest_record_time BIGINT,
  biz_name STRING,
  data_source STRING,
  user_id STRING,
  user_name STRING,
  age SMALLINT,
  sex TINYINT,
  family STRING,
  sign STRING,
  user_level SMALLINT,
  vip_level SMALLINT,
  constellation STRING,
  hometown STRING,
  occupation STRING,
  follow_count INT,
  fans_count INT,
  income_app_coin STRING,
  cost_app_coin STRING,
  location STRING,
  total_live_count INT,
  total_live_time BIGINT,
  total_audience_count BIGINT,
  total_gift_count INT,
  total_income DOUBLE,
  total_message_count INT,
  last_start_time BIGINT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_user_new_recv_gift_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_user_new_recv_message_snapshot(
  latest_record_time BIGINT,
  biz_name STRING,
  data_source STRING,
  user_id STRING,
  user_name STRING,
  age SMALLINT,
  sex TINYINT,
  family STRING,
  sign STRING,
  user_level SMALLINT,
  vip_level SMALLINT,
  constellation STRING,
  hometown STRING,
  occupation STRING,
  follow_count INT,
  fans_count INT,
  income_app_coin STRING,
  cost_app_coin STRING,
  location STRING,
  total_live_count INT,
  total_live_time BIGINT,
  total_audience_count BIGINT,
  total_gift_count INT,
  total_income DOUBLE,
  total_message_count INT,
  last_start_time BIGINT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_user_new_recv_message_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_audience_send_gift_active_snapshot(
  biz_name STRING,
  data_source STRING,
  audience_id STRING,
  audience_name STRING,
  send_count INT,
  send_value DOUBLE,
  send_anchor_count INT,
  send_live_count INT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_audience_send_gift_active_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_audience_send_gift_new_snapshot(
  biz_name STRING,
  data_source STRING,
  audience_id STRING,
  audience_name STRING,
  send_count INT,
  send_value DOUBLE,
  send_anchor_count INT,
  send_live_count INT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_audience_send_gift_new_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_audience_send_message_active_snapshot(
  biz_name STRING,
  data_source STRING,
  audience_id STRING,
  audience_name STRING,
  send_count INT,
  send_anchor_count INT,
  send_live_count INT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_audience_send_message_active_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_audience_send_message_new_snapshot(
  biz_name STRING,
  data_source STRING,
  audience_id STRING,
  audience_name STRING,
  send_count INT,
  send_anchor_count INT,
  send_live_count INT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_audience_send_message_new_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_audience_active_snapshot(
  biz_name STRING,
  data_source STRING,
  audience_id STRING,
  audience_name STRING,
  send_gift_count INT,
  send_gift_value DOUBLE,
  send_gift_anchor_count INT,
  send_gift_live_count INT,
  send_message_count INT,
  send_message_anchor_count INT,
  send_message_live_count INT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_audience_active_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_audience_snapshot(
  biz_name STRING,
  data_source STRING,
  audience_id STRING,
  audience_name STRING,
  send_gift_count INT,
  send_gift_value DOUBLE,
  send_gift_anchor_count INT,
  send_gift_live_count INT,
  send_message_count INT,
  send_message_anchor_count INT,
  send_message_live_count INT
)
PARTITIONED BY (dt STRING, hour STRING, app_id STRING)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_audience_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_platform_snapshot(
  record_time BIGINT,
  dt STRING,
  hour STRING,
  biz_name STRING,
  data_source STRING,
  live_count INT,
  live_time BIGINT,
  active_user_count INT,
  new_active_user_count INT,
  recv_gift_live_count INT,
  recv_gift_user_count INT,
  new_recv_gift_user_count INT,
  recv_message_live_count INT,
  recv_message_user_count INT,
  new_recv_message_user_count INT,
  income DOUBLE,
  gift_count BIGINT,
  message_count BIGINT,
  audience_count BIGINT,
  send_gift_audience_count INT,
  new_send_gift_audience_count INT,
  send_message_audience_count INT,
  new_send_message_audience_count INT,
  active_audience_count INT,
  violation_count INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_platform_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_platform_daily_snapshot(
  record_time BIGINT,
  dt STRING,
  biz_name STRING,
  data_source STRING,
  live_count INT,
  live_time BIGINT,
  active_user_count INT,
  new_active_user_count INT,
  recv_gift_live_count INT,
  recv_gift_user_count INT,
  new_recv_gift_user_count INT,
  recv_message_live_count INT,
  recv_message_user_count INT,
  new_recv_message_user_count INT,
  income DOUBLE,
  gift_count BIGINT,
  message_count BIGINT,
  audience_count BIGINT,
  send_gift_audience_count INT,
  new_send_gift_audience_count INT,
  send_message_audience_count INT,
  new_send_message_audience_count INT,
  active_audience_count INT,
  violation_count INT
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_platform_daily_snapshot'
"

executeHiveCommand "
CREATE EXTERNAL TABLE IF NOT EXISTS live_p2.tbl_ex_api_uri_snapshot(
  record_time BIGINT,
  trace_id STRING,
  biz_name STRING,
  data_source STRING,
  uri STRING,
  host_ip STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '${p2_location_live_snapshot}/tbl_ex_api_uri_snapshot'
"

echo "################# 创建 hive live_p2 tables end  ########################"

