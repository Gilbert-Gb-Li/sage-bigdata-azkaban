#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh


echo "################# 创建 mysql live_p2 tables start  ########################"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS `tbl_live_platform_monitor_region_audience` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_package_name` varchar(50) DEFAULT '',
  `data_source` varchar(10) DEFAULT '' ,
  `location` varchar(50) DEFAULT NULL,
  `audience_count_max` int(10) DEFAULT NULL ,
  `time_string` varchar(12) DEFAULT NULL ,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS `tbl_live_platform_monitor_minute` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_package_name` varchar(50) DEFAULT '',
  `data_source` varchar(10) DEFAULT '',
  `active_user_num_history` int(10) DEFAULT NULL,
  `live_count_history` int(10) DEFAULT NULL ,
  `gift_money_history` double(10,2) DEFAULT NULL,
  `audience_gift_num_history` int(10) DEFAULT NULL,
  `violating_num_history` int(10) DEFAULT NULL,
  `time_string` varchar(12) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS `tbl_live_platform_monitor_hour` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_package_name` varchar(100) DEFAULT '',
  `data_source` varchar(10) DEFAULT '',
  `user_num` int(10) DEFAULT NULL ,
  `gift_money` double(10,2) DEFAULT NULL ,
  `audience_gift_num` int(10) DEFAULT NULL ,
  `online_user_num_max` int(10) DEFAULT NULL ,
  `violating_num` int(10) DEFAULT NULL ,
  `time_string` varchar(12) DEFAULT NULL ,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"

execSqlOnMysql "
CREATE TABLE IF NOT EXISTS `tbl_live_platform_monitor_day` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `app_package_name` varchar(50) DEFAULT '',
  `data_source` varchar(10) DEFAULT '' ,
  `live_count` int(10) DEFAULT NULL,
  `active_user_num` int(10) DEFAULT NULL ,
  `active_user_num_month_natural` int(10) DEFAULT NULL ,
  `gift_money` double(10,2) DEFAULT NULL ,
  `audience_gift_num` int(10) DEFAULT NULL ,
  `audience_gift_num_month_natural` int(10) DEFAULT NULL ,
  `online_user_num_max` int(10) DEFAULT NULL ,
  `violating_num` int(10) DEFAULT NULL ,
  `time_string` varchar(12) DEFAULT NULL ,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
"
echo "################# 创建 mysql live_p2 tables end  ########################"