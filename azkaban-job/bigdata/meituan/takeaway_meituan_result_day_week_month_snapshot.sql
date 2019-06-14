CREATE EXTERNAL TABLE if not exists `bigdata.takeaway_meituan_result_day_week_month_snapshot`(
  `meituan_quick_num` bigint COMMENT '商家数美团快送',
  `meituan_special_num` bigint COMMENT '商家数美团专送',
  `self_take_num` bigint COMMENT '商家数无配送方式',
  `new_shop_num` bigint COMMENT '新增商家数按平台统计',
  `new_food_num` bigint COMMENT '新增商家数美食类',
  `new_not_foot_num` bigint COMMENT '新增商家数非美食类',
  `time_cecle` bigint COMMENT '统计周期，1：天，2：周，3：月'
) COMMENT 'takeaway_meituan_result_week'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS ORC
  LOCATION '/data/meituan/snapshot/takeaway_meituan_result_week';