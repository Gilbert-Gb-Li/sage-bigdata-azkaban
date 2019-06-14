CREATE EXTERNAL TABLE if not exists `bigdata.meituan_financial_new_shop_menu_list_snapshot`(
  `unique_id` string COMMENT 'shop_id + menu_id + 菜品分量 作为唯一id',
  `shop_id` string COMMENT '商家id',
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
  `restaurant_id` string COMMENT '商家id',
  `ias_timestamp` string COMMENT '时间戳',
  `meta_name` string
) COMMENT 'new shop menu list'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
  LINES TERMINATED BY '\n'
  STORED AS ORC
  LOCATION '/data/meituan/snapshot/meituan_financial_new_shop_menu_list';