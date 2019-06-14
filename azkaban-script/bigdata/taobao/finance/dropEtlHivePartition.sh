#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql=`cat << EOF
use bigdata;

alter table bigdata.taobao_boyu_goods_origin drop IF EXISTS partition(dt='${date}');
alter table bigdata.taobao_boyu_goods_active_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.taobao_boyu_goods_all_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.taobao_boyu_goods_brand_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.taobao_boyu_goods_new_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.taobao_boyu_goods_statistics_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.taobao_boyu_shop_all_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.taobao_boyu_shop_new_snapshot drop IF EXISTS partition(dt='${date}');

EOF`

executeHiveCommand "${hive_sql}"