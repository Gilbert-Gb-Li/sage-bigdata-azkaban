CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_update_video_data_origin` (
  `appPackageName` string COMMENT 'miaopai',
  `app_id` string COMMENT 'Appid 默认为空',
  `user_id` string COMMENT '主播ID',
  `video_url_status` string COMMENT '0:初始状态1：可用2：不可用默认0',
  `url_failure_time` string COMMENT 'app',
  `u_id` string COMMENT '跨APP的全局唯一标识。全局指的是互联网不良内容巡检平台。可采用app_id+用户id',
  `tenant_id` string COMMENT '租户id',
  `ai_audit_time` string COMMENT 'AI最近审核时间',
  `ai_status` string COMMENT '0：未审核，1：AI已审核。默认值：0',
  `manual_status` string COMMENT '0：未审核，1：人工审核未违规 2：人工审核违规。默认值：0',
  `ai_version` string COMMENT 'AI审核版本 默认v0',
  `time_stamp` string COMMENT '时间戳'
  ) COMMENT '版权更新视频数据'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	LOCATION '/data/copyrightcloud/secphoto/origin/update-video-data/';
