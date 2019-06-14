add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
CREATE EXTERNAL TABLE IF NOT EXISTS bigdata.meituan_result_wide_day_snapshot_es( 
`new_shop_count_day` bigint COMMENT '自然日新增商家数按平台统计', 
`new_shop_count_food_day` bigint COMMENT '自然日新增商家数美食类按平台统计', 
`new_shop_count_notfood_day` bigint COMMENT '自然日新增商家数非美食类按平台统计', 
`shop_count_special_day` bigint COMMENT '自然日美团专送商家数按平台统计',
`shop_count_quick_day` bigint COMMENT '自然日美团快送商家数按平台统计',  
`shop_count_self_day` bigint COMMENT '自然日无配送商家数按平台统计', 
`add_shop_count_platform_week` bigint COMMENT '近7日新增商家数按平台统计', 
`add_shop_count_cate_week` bigint COMMENT '近7日新增商家数美食类按平台统计', 
`add_shop_count_nocate_week` bigint COMMENT '近7日新增商家数非美食类按平台统计',
`shop_count_special_week` bigint COMMENT '近7日美团专送商家数按平台统计',  
`shop_count_quick_week` bigint COMMENT '近7日美团快送商家数按平台统计', 
`shop_count_self_week` bigint COMMENT '近7日无配送方式商家数按平台统计', 
`add_shop_count_platform_month` bigint COMMENT '近30日新增商家数按平台统计', 
`add_shop_count_cate_month` bigint COMMENT '近30日新增商家数美食类按平台统计', 
`add_shop_count_nocate_month` bigint COMMENT '近30日新增商家数非美食类按平台统计',
`shop_count_special_month` bigint COMMENT '近30日美团专送商家数按平台统计', 
`shop_count_quick_month` bigint COMMENT '近30日美团快送商家数按平台统计', 
`shop_count_self_month` bigint COMMENT '近30日无配送方式商家数按平台统计', 
`shop_count_platform_day` bigint COMMENT '自然日商家数按平台统计',
`order_count_platform_month` bigint COMMENT '近30日订单数按平台统计', 
`order_count_special_month` bigint COMMENT '近30日美团专送订单数按平台统计',
`order_count_quick_month` bigint COMMENT '近30日美团快送订单数按平台统计',  
`order_count_self_month` bigint COMMENT '近30日无配送订单数按平台统计', 
`total_transactions_platform_month` double COMMENT '近30日账面交易总额按平台统计',
`stat_date` string COMMENT '日期index',
`meta_app_name` string COMMENT 'app应用名字') 
ROW FORMAT SERDE 'org.elasticsearch.hadoop.hive.EsSerDe' 
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler' 
TBLPROPERTIES 
( 'es.resource' = 'takeaway_meituan_result_d_{stat_date}/data', 
'es.nodes'='mvp-hadoop40,mvp-hadoop41,mvp-hadoop42', 
'es.port'='9200', 
'es.nodes.wan.only'='true' );