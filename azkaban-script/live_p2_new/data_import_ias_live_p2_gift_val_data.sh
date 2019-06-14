#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh


echo "################# dump mysql 数据 start ########################"

tmp_live_gift_val_file=${tmpDir}/live_gift_val_file.txt

rm -rf ${tmp_live_gift_val_file}

mysql_sql="
SELECT a.id,a.app_id,a.data_source,a.gift_id,a.gift_name,a.gift_url,a.gift_price,a.unit,a.gmt_get,
       b.exchange_rate,b.crrent_type,
       a.gift_price*b.exchange_rate AS gift_unit_val
FROM
(SELECT id,app_id,data_source,gift_id,gift_name,gift_url,gift_price,unit,gmt_get from ${gift_mysql_db}.live_gift where available=1 ) as a
LEFT JOIN
(SELECT id,app_id,virtual_currencyt_name,exchange_rate,crrent_type from ${gift_mysql_db}.live_gift_rate) as b
ON a.app_id=b.app_id AND a.unit=b.virtual_currencyt_name
"
echo "${mysql_sql}"

${mysql} -h${gift_mysql_host} -P${gift_mysql_port} -u${gift_mysql_user} -p${gift_mysql_password} --default-character-set=utf8 -e "${mysql_sql}" > ${tmp_live_gift_val_file}

echo "################# dump mysql 数据 end ########################"


echo "################# 删除快照、创建快照table、加载数据到ias_p2.tbl_ex_gift_val_data表   start ########################"

gift_val_table="tbl_ex_gift_val_data"

drop_table_gift_val="drop table ias_p2.${gift_val_table};"

create_table_gift_val="
CREATE TABLE IF NOT EXISTS ias_p2.${gift_val_table}(
  id INT,
  app_id STRING,
  data_source STRING,
  gift_id STRING,
  gift_name STRING,
  gift_url STRING,
  gift_price STRING,
  unit STRING,
  gmt_get INT,
  exchange_rate DOUBLE,
  currency_type INT,
  gift_unit_val DOUBLE
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
"

executeHiveCommand "${drop_table_gift_val} ${create_table_gift_val} LOAD DATA LOCAL INPATH '${tmp_live_gift_val_file}' INTO TABLE ias_p2.${gift_val_table};"

rm -rf ${tmp_live_gift_val_file}

echo "################# 删除快照、创建快照table、加载数据到tbl_ex_gift_val_data 表   end ########################"

