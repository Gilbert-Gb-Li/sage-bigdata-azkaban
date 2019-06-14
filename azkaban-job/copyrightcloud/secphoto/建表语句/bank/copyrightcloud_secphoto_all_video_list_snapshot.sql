CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_all_video_list_snapshot` (
  `appPackageName` string COMMENT 'miaopai',
  `ias_timestamp` string COMMENT '1547195373209',
  `video_id` string COMMENT '作品ID',
  `video_title` string COMMENT '作品标题',
  `video_url` string COMMENT '作品连接',
  `content_type` string COMMENT '内容类型',
  `user_id` string COMMENT '作品发布者ID',
  `user_nickname` string COMMENT '作品发布者昵称',
  `video_public_time` string COMMENT '作品发布时间'
  ) COMMENT '作品列表'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	STORED AS ORC
	LOCATION '/data/copyrightcloud/secphoto/snapshot/all_video_list/';