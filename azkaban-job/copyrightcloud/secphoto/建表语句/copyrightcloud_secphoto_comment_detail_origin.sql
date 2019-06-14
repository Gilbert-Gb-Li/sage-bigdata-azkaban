CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_comment_detail_origin` (
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
	LOCATION '/data/copyrightcloud/secphoto/origin/short-video-comment/';