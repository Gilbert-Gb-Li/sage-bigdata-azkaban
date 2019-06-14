CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_comment_detail_snapshot` (
  `appPackageName` string COMMENT 'miaopai',
  `ias_timestamp` string COMMENT '1547195373209',
  `video_id` string COMMENT '评论作品ID',
  `user_id` string COMMENT '评论用户ID',
  `comment_user_nickname` string COMMENT '评论用户昵称',
  `comment_content` string COMMENT '评论内容',
  `comment_time` string COMMENT '评论发布时间'
  ) COMMENT '评论详情'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	STORED AS ORC
	LOCATION '/data/copyrightcloud/secphoto/snapshot/daily-short-video-comment/';