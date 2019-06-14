CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_update_user_data_origin` (
  `appPackageName` string COMMENT '包名',
  `app_id` string COMMENT 'Appid 默认为空',
  `user_id` string COMMENT '主播ID',
  `u_id` string COMMENT '跨APP的全局唯一标识。全局指的是互联网不良内容巡检平台。可采用app_id+用户id',
  `user_pure` string COMMENT '白名单',
  `time_stamp` string COMMENT '时间戳'
  ) COMMENT '版权更新用户数据'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	LOCATION '/data/copyrightcloud/secphoto/origin/update-user-data/';
