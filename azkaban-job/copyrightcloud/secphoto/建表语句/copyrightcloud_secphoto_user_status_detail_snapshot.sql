CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_user_status_detail_snapshot` (
	`appVersion` string COMMENT '8.4.1',
	`app_package_name` string COMMENT '包名',
	`resourceKey` string COMMENT 'b32e160e9cf4a7524a4567ee693f2c9237798cdf',
	`user_name` string COMMENT '用户名',
	`user_id` string COMMENT '用户id',
	`user_dp` string COMMENT '用户头像',
	`user_follow_num` string COMMENT '用户关注数',
	`user_fans_num` string COMMENT '用户粉丝数',
	`user_video_num` string COMMENT '用户作品数',
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
	LOCATION '/data/copyrightcloud/secphoto/snapshot/detail_user_status_list/';