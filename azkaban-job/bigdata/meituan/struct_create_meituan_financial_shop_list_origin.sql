CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.meituan_financial_shop_list_origin` (
  `cloudServiceId` string COMMENT '云服务ID 2572603b6c004f169ccaa9d898162125',
  `appVersion` string COMMENT '8.4.1',
  `appPackageName` string COMMENT 'me.ele',
  `dataSource` string COMMENT 'app',
  `data_schema` string COMMENT 'shop_list',
  `resourceKey` string COMMENT 'me.ele ID_LIST me.ele.application.ui.home.d 542c985e2f2d482544174f5dadb6d5463518d203 b32e160e9cf4a7524a4567ee693f2c9237798cdf',
  `spiderVersion` string COMMENT '3.0.15',
  `containerId` string COMMENT 'ed2ffd64-339a-4846-a73f-99f1f1744914',
  `dataType` string COMMENT '1',
  `ias_timestamp` string COMMENT '1547195373209',
  `shop_name` string COMMENT '商家名称 例如：100',
  `waimai_type` string COMMENT '外卖类型 1.美团 2.饿了么',
  `shop_id` string COMMENT '商家id',
  `restaurant_id` string COMMENT '24Hb变化一次的shop id E10727004202182749673',
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
  `current_page` string,
  `meta_name` string
  ) COMMENT '商家列表'
  PARTITIONED BY (`dt` string)
  ROW FORMAT DELIMITED
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	LOCATION '/data/meituan/origin/shop_list/';