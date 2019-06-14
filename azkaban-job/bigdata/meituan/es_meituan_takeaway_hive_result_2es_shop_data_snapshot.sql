  add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
  add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
CREATE EXTERNAL TABLE if not exists`bigdata.takeaway_meituan_transactions_shop_day_es`(
  `shop_id` string COMMENT '商家id',
  `order_num` bigint COMMENT '总订单数',
  `total_transactions_shop`double COMMENT '商家的交易总额',
  `stat_date` string COMMENT '日期index',
  `meta_app_name` string  COMMENT 'app应用名字'
    )
 ROW FORMAT SERDE
  'org.elasticsearch.hadoop.hive.EsSerDe'
 STORED BY
  'org.elasticsearch.hadoop.hive.EsStorageHandler'
 TBLPROPERTIES (
  'es.resource' = 'takeaway_meituan_transactions_d_{stat_date}/data',
  'es.nodes'='mvp-hadoop40,mvp-hadoop41,mvp-hadoop42',
  'es.port'='9200',
  'es.nodes.wan.only'='true'
);