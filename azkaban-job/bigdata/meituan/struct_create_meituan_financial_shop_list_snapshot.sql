CREATE EXTERNAL TABLE `bigdata.meituan_financial_shop_list_snapshot`(
  `restaurant_id` string COMMENT '商家24H变化名称',
  `shop_name` string COMMENT '商家名称 例如：100',
  `shop_id` string COMMENT '商家id',
  `address` string COMMENT '地址',
  `shop_support_self_taking` string COMMENT '是否支持自取',
  `phone` string COMMENT '商家电话',
  `categories` string COMMENT '分类 如：简餐、火锅',
  `open_hours` string COMMENT '营业时间',
  `shop_logo` string COMMENT '商家Logo',
  `shop_rating` string COMMENT '商家评分',
  `shop_recent_food_popularity` bigint COMMENT '商家月售量',
  `shop_min_order_amount` double COMMENT '起送价',
  `shop_average_price` double COMMENT '人均价',
  `shop_delivery_mode` string COMMENT '配送方式',
  `shop_label_icon` string COMMENT '商家logo的右上角icon有可能是品牌字样，或者其他',
  `shop_delivery_price` string COMMENT '配送价格',
  `latitude` double COMMENT '维度',
  `longitude` double COMMENT '经度',
  `ias_timestamp` string COMMENT '时间戳',
  `current_page` string,
  `province` string COMMENT '省份',
  `city` string COMMENT '城市',
  `meta_name` string
)
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS ORC
  LOCATION '/data/meituan/snapshot/meituan_financial_shop_list';
