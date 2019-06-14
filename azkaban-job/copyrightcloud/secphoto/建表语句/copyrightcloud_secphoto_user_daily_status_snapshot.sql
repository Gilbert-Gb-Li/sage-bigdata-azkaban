CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_user_daily_status_snapshot` (
  `appPackageName` string COMMENT 'miaopai',
  `user_id` string COMMENT '主播ID',
  `new_user` string COMMENT '是否新增主播',
  `increase_user` string COMMENT '是否粉丝暴增',
  `user_status` string COMMENT '主播是否违规',
  `user_pure` string COMMENT '是否在白名单'
  ) COMMENT '主播列表'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	STORED AS ORC
	LOCATION '/data/copyrightcloud/secphoto/snapshot/daily_user_status_list/';