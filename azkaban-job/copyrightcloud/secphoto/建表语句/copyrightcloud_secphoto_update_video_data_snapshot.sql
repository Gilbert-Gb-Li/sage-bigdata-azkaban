CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.copyrightcloud_secphoto_update_video_data_snapshot` (
  `appPackageName` string COMMENT '包名',
  `user_id` string COMMENT '主播ID',
  `manual_status` string COMMENT '0：未审核，1：人工审核未违规 2：人工审核违规。默认值：0',
  `time_stamp` string COMMENT '时间戳'
  ) COMMENT '版权更新视频数据'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	STORED AS ORC
	LOCATION '/data/copyrightcloud/secphoto/snapshot/update-video-data/';
