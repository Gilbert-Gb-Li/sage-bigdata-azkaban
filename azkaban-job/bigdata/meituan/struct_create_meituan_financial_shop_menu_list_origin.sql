CREATE EXTERNAL TABLE IF NOT EXISTS `bigdata.meituan_financial_shop_menu_list_origin`(
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
  `menu_name` string COMMENT '菜品名称 例如：100',
  `menu_id` string COMMENT '菜品id',
  `menu_origin_price` double COMMENT '菜品原价',
  `menu_price` double COMMENT '菜品价格',
  `menu_quantity` string COMMENT '菜品分量  大中小份',
  `menu_packing_fee` double COMMENT '打包费',
  `menu_packing_num` int COMMENT '打包数量',
  `menu_is_off_sell` string COMMENT '菜品是否售完',
  `menu_month_sales` bigint COMMENT '菜品月售量',
  `menu_rating` double COMMENT '菜品评分',
  `menu_rating_count` bigint COMMENT '菜品评分次数',
  `menu_satisfy_rate` double COMMENT '菜品好评率',
  `menu_food_spec` string COMMENT '菜品规格如：酸奶',
  `menu_max_promotion_quantity_detail_text` string COMMENT '菜品优惠信息',
  `menu_promotion_is_Must_super_vip` string COMMENT '菜品优惠是否需要supervip',
  `restaurant_id` string COMMENT '商家24H变化id',
  `shop_id` string COMMENT '商家id',
  `meta_name` string
  ) COMMENT 'shop menu list'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED 
	FIELDS TERMINATED BY '\t'
	LINES TERMINATED BY '\n'
	LOCATION '/data/meituan/origin/shop_menu_list/';
  
  