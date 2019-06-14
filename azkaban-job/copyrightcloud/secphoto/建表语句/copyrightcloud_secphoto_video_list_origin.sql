CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_video_list_origin` (
  `cloudServiceId` string COMMENT '云服务ID 2572603b6c004f169ccaa9d898162125',
  `appVersion` string COMMENT '8.4.1',
  `appPackageName` string COMMENT 'miaopai',
  `dataSource` string COMMENT 'app',
  `data_schema` string COMMENT 'short-video-user',
  `resourceKey` string COMMENT 'b32e160e9cf4a7524a4567ee693f2c9237798cdf',
  `spiderVersion` string COMMENT '3.0.15',
  `containerId` string COMMENT 'ed2ffd64-339a-4846-a73f-99f1f1744914',
  `dataType` string COMMENT '2',
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
	LOCATION '/data/copyrightcloud/secphoto/origin/short-video-data/';