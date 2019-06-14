CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_user_list_snapshot` (
  `appVersion` string COMMENT '8.4.1',
  `appPackageName` string COMMENT 'miaopai',
  `resourceKey` string COMMENT 'b32e160e9cf4a7524a4567ee693f2c9237798cdf',
  `ias_timestamp` string COMMENT '1547195373209',
  `user_id` string COMMENT '主播ID',
  `user_avatar` string COMMENT '主播头像',
  `user_nickname` string COMMENT '主播昵称',
  `user_attention_num` string COMMENT '主播关注数',
  `user_follower_num` string COMMENT '主播粉丝数',
  `user_works_num` string COMMENT '主播作品数'
  ) COMMENT '主播列表'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	STORED AS ORC
	LOCATION '/data/copyrightcloud/secphoto/snapshot/daily_user_list/';