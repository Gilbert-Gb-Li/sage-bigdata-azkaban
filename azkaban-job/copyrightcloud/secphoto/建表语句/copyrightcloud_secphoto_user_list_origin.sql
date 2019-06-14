CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_user_list_origin` (
  `cloudServiceId` string COMMENT '云服务ID 2572603b6c004f169ccaa9d898162125',
  `appVersion` string COMMENT '8.4.1',
  `appPackageName` string COMMENT 'miaopai',
  `dataSource` string COMMENT 'app',
  `data_schema` string COMMENT 'short-video-user',
  `resourceKey` string COMMENT 'b32e160e9cf4a7524a4567ee693f2c9237798cdf',
  `spiderVersion` string COMMENT '3.0.15',
  `containerId` string COMMENT 'ed2ffd64-339a-4846-a73f-99f1f1744914',
  `dataType` string COMMENT '1',
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
	LOCATION '/data/copyrightcloud/secphoto/origin/short-video-user/';