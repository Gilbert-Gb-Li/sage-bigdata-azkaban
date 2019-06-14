CREATE EXTERNAL TABLE if not exists`bigdata.takeaway_meituan_transactions_shop_result_snapshot`(
  `shop_id` string COMMENT '商家id',
  `order_num` string COMMENT '总订单数',
  `total_transactions_shop`double COMMENT '商家的交易总额'
) COMMENT 'takeaway_meituan_transactions_shop_result'
PARTITIONED BY (`dt` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS ORC
  LOCATION '/data/meituan/snapshot/takeaway_meituan_transactions_shop_result';
