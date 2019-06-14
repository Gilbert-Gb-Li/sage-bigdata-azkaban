CREATE EXTERNAL TABLE if not exists `bigdata.takeaway_meituan_result_order_platform_snapshot`(
  `total_transactions_platform` double COMMENT '平台的交易总额',
  `shop_count_platform` bigint COMMENT '平台商家数',
  `order_num_platform` bigint COMMENT '平台订单数',
  `meituan_quick_num` bigint COMMENT '平台范围商家数美团快送订单数',
  `meituan_special_num` bigint COMMENT '平台范围商家数美团专送订单数',
  `self_take_num` bigint COMMENT '平台范围商家数无配送方式订单数'
) COMMENT 'takeaway_meituan_result_order_month_platform'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS ORC
  LOCATION '/data/meituan/snapshot/takeaway_meituan_result_order_month_platform';