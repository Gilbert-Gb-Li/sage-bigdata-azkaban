#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh


echo "################# 创建 mysql live_p2 tables start  ########################"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_app_info (
  biz_name VARCHAR(100) PRIMARY KEY,
  name VARCHAR(40),
  version VARCHAR(40),
  version_code INT,
  download_url VARCHAR(256),
  size INT,
  hash VARCHAR(60),
  type INT,
  icon_url VARCHAR(256),
  location LONGTEXT DEFAULT '',
  ip_info LONGTEXT DEFAULT '',
  contact_info TEXT DEFAULT ''
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_app_location (
  id INT PRIMARY KEY AUTO_INCREMENT,
  biz_name VARCHAR(256),
  name VARCHAR(40),
  version VARCHAR(40),
  version_code INT,
  location_type VARCHAR(20),
  location VARCHAR(255),
  ip VARCHAR(20)
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_monitor_app_hour_data (
  id INT PRIMARY KEY AUTO_INCREMENT,
  location VARCHAR(30),
  dt VARCHAR(11),
  hour TINYINT,
  biz_name VARCHAR(256),
  data_source VARCHAR(20),
  name VARCHAR(40),
  live_count INT,
  audience_count BIGINT,
  income DOUBLE,
  violation_count INT,
  live_count_avg INT,
  audience_count_avg BIGINT,
  income_avg DOUBLE,
  violation_count_avg INT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_monitor_hour_data (
  id INT PRIMARY KEY AUTO_INCREMENT,
  location VARCHAR(30),
  dt VARCHAR(11),
  hour TINYINT,
  app_count INT,
  live_count INT,
  audience_count BIGINT,
  income DOUBLE,
  violation_count INT,
  live_count_avg INT,
  audience_count_avg BIGINT,
  income_avg DOUBLE,
  violation_count_avg INT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_monitor_app_daily_data (
  id INT PRIMARY KEY AUTO_INCREMENT,
  location VARCHAR(30),
  dt VARCHAR(11),
  biz_name VARCHAR(256),
  data_source VARCHAR(20),
  name VARCHAR(40),
  live_count INT,
  audience_count BIGINT,
  income DOUBLE,
  violation_count INT,
  live_count_avg INT,
  audience_count_avg BIGINT,
  income_avg DOUBLE,
  violation_count_avg INT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_monitor_daily_data (
  id INT PRIMARY KEY AUTO_INCREMENT,
  location VARCHAR(30),
  dt VARCHAR(11),
  app_count INT,
  live_count INT,
  audience_count BIGINT,
  income DOUBLE,
  violation_count INT,
  live_count_avg INT,
  audience_count_avg BIGINT,
  income_avg DOUBLE,
  violation_count_avg INT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_monitor_violation_user (
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt VARCHAR(11),
  hour TINYINT,
  biz_name VARCHAR(256),
  data_source VARCHAR(20),

  search_id VARCHAR(100),
  order_id VARCHAR(30),

  video_url VARCHAR(256),
  video_length INT,
  start_time BIGINT,
  end_time BIGINT,
  result_code INT,

  income DOUBLE,
  gift_count INT,
  message_count INT,

  max_audience_count INT,
  min_audience_count INT,
  avg_audience_count INT,

  app_name
  version VARCHAR(40),
  version_code INT,
  apk_url VARCHAR(256),
  apk_size INT,
  apk_hash VARCHAR(60),
  app_type INT,
  app_icon_url VARCHAR(256),
  app_location LONGTEXT,
  app_ip_info LONGTEXT,

  user_id VARCHAR(100),
  user_name VARCHAR(100),
  age SMALLINT,
  sex TINYINT,
  family VARCHAR(100),
  sign VARCHAR(1000),
  user_level SMALLINT,
  vip_level SMALLINT,
  constellation VARCHAR(50),
  hometown VARCHAR(50),
  occupation VARCHAR(50),
  follow_count INT,
  fans_count INT,
  income_app_coin VARCHAR(50),
  cost_app_coin VARCHAR(50),
  location VARCHAR(50),

  user_avatar_url VARCHAR(256) DEFAULT '',
  contact_info TEXT DEFAULT '',

  is_valid TINYINT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_monitor_sex_app (
  biz_name VARCHAR(100),
  app_name VARCHAR(40),
  version VARCHAR(40),
  version_code INT,
  apk_url VARCHAR(256),
  apk_size INT,
  apk_hash VARCHAR(60),
  app_type INT,
  app_icon_url VARCHAR(256),
  location LONGTEXT,
  ip_info LONGTEXT,
  contact_info TEXT,

  start_time BIGINT,
  end_time BIGINT,
  audience_count BIGINT,
  income DOUBLE,
  gift_count INT,
  message_count INT,
  live_count INT,
  video TEXT,
  dt CHAR(11)
)
"


execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_insight_user_rank(
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt_section VARCHAR(23),
  rank_point DOUBLE,
  biz_name VARCHAR(256),
  data_source VARCHAR(20),
  user_id VARCHAR(100),
  user_name VARCHAR(100),
  age SMALLINT,
  sex TINYINT,
  family VARCHAR(100),
  sign VARCHAR(1000),
  user_level SMALLINT,
  vip_level SMALLINT,
  constellation VARCHAR(50),
  hometown VARCHAR(50),
  occupation VARCHAR(50),
  follow_count INT,
  fans_count INT,
  income_app_coin VARCHAR(50),
  cost_app_coin VARCHAR(50),
  location VARCHAR(50),
  total_live_count INT,
  total_live_time BIGINT,
  total_audience_count BIGINT,
  total_gift_count INT,
  total_income DOUBLE,
  total_message_count INT,
  last_start_time BIGINT
)
"
execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_insight_audience_rank(
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt_section VARCHAR(23),
  rank_point DOUBLE,
  biz_name VARCHAR(256),
  data_source VARCHAR(20),
  audience_id VARCHAR(100),
  audience_name VARCHAR(100),
  send_gift_count INT,
  send_gift_value DOUBLE,
  send_gift_anchor_count INT,
  send_gift_live_count INT,
  send_message_count INT,
  send_message_anchor_count INT,
  send_message_live_count INT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_insight_platform_daily_data(
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt VARCHAR(11),
  biz_name VARCHAR(256),
  data_source VARCHAR(20),
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
  active_audience_count INT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_insight_platform_rank(
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt_section VARCHAR(23),
  rank_point DOUBLE,
  biz_name VARCHAR(256),
  data_source VARCHAR(20),
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
  active_audience_count INT
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_insight_user_remain(
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt VARCHAR(11),
  biz_name VARCHAR(100),
  data_source VARCHAR(20),
  new_count INT DEFAULT -1,
  day_1 INT DEFAULT -1,
  day_2 INT DEFAULT -1,
  day_3 INT DEFAULT -1,
  day_4 INT DEFAULT -1,
  day_5 INT DEFAULT -1,
  day_6 INT DEFAULT -1,
  day_7 INT DEFAULT -1,
  day_8 INT DEFAULT -1,
  day_9 INT DEFAULT -1,
  day_10 INT DEFAULT -1,
  day_11 INT DEFAULT -1,
  day_12 INT DEFAULT -1,
  day_13 INT DEFAULT -1,
  day_14 INT DEFAULT -1,
  day_15 INT DEFAULT -1,
  day_16 INT DEFAULT -1,
  day_17 INT DEFAULT -1,
  day_18 INT DEFAULT -1,
  day_19 INT DEFAULT -1,
  day_20 INT DEFAULT -1,
  day_21 INT DEFAULT -1,
  day_22 INT DEFAULT -1,
  day_23 INT DEFAULT -1,
  day_24 INT DEFAULT -1,
  day_25 INT DEFAULT -1,
  day_26 INT DEFAULT -1,
  day_27 INT DEFAULT -1,
  day_28 INT DEFAULT -1,
  day_29 INT DEFAULT -1,
  day_30 INT DEFAULT -1,
  UNIQUE (dt, biz_name, data_source)
)
"


execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_insight_send_gift_audience_remain(
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt VARCHAR(11),
  biz_name VARCHAR(100),
  data_source VARCHAR(20),
  new_count INT DEFAULT -1,
  day_1 INT DEFAULT -1,
  day_2 INT DEFAULT -1,
  day_3 INT DEFAULT -1,
  day_4 INT DEFAULT -1,
  day_5 INT DEFAULT -1,
  day_6 INT DEFAULT -1,
  day_7 INT DEFAULT -1,
  day_8 INT DEFAULT -1,
  day_9 INT DEFAULT -1,
  day_10 INT DEFAULT -1,
  day_11 INT DEFAULT -1,
  day_12 INT DEFAULT -1,
  day_13 INT DEFAULT -1,
  day_14 INT DEFAULT -1,
  day_15 INT DEFAULT -1,
  day_16 INT DEFAULT -1,
  day_17 INT DEFAULT -1,
  day_18 INT DEFAULT -1,
  day_19 INT DEFAULT -1,
  day_20 INT DEFAULT -1,
  day_21 INT DEFAULT -1,
  day_22 INT DEFAULT -1,
  day_23 INT DEFAULT -1,
  day_24 INT DEFAULT -1,
  day_25 INT DEFAULT -1,
  day_26 INT DEFAULT -1,
  day_27 INT DEFAULT -1,
  day_28 INT DEFAULT -1,
  day_29 INT DEFAULT -1,
  day_30 INT DEFAULT -1,
  UNIQUE (dt, biz_name, data_source)
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_insight_send_message_audience_remain(
  id INT PRIMARY KEY AUTO_INCREMENT,
  dt VARCHAR(11),
  biz_name VARCHAR(100),
  data_source VARCHAR(20),
  new_count INT DEFAULT -1,
  day_1 INT DEFAULT -1,
  day_2 INT DEFAULT -1,
  day_3 INT DEFAULT -1,
  day_4 INT DEFAULT -1,
  day_5 INT DEFAULT -1,
  day_6 INT DEFAULT -1,
  day_7 INT DEFAULT -1,
  day_8 INT DEFAULT -1,
  day_9 INT DEFAULT -1,
  day_10 INT DEFAULT -1,
  day_11 INT DEFAULT -1,
  day_12 INT DEFAULT -1,
  day_13 INT DEFAULT -1,
  day_14 INT DEFAULT -1,
  day_15 INT DEFAULT -1,
  day_16 INT DEFAULT -1,
  day_17 INT DEFAULT -1,
  day_18 INT DEFAULT -1,
  day_19 INT DEFAULT -1,
  day_20 INT DEFAULT -1,
  day_21 INT DEFAULT -1,
  day_22 INT DEFAULT -1,
  day_23 INT DEFAULT -1,
  day_24 INT DEFAULT -1,
  day_25 INT DEFAULT -1,
  day_26 INT DEFAULT -1,
  day_27 INT DEFAULT -1,
  day_28 INT DEFAULT -1,
  day_29 INT DEFAULT -1,
  day_30 INT DEFAULT -1,
  UNIQUE (dt, biz_name, data_source)
)
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS tbl_live_p2_api_uri(
  id INT PRIMARY KEY AUTO_INCREMENT,
  biz_name VARCHAR(100),
  data_source VARCHAR(20),
  host_ip VARCHAR(128),
  count BIGINT,
  lastest_update_time BIGINT
)
"

echo "################# 创建 mysql live_p2 tables end  ########################"

