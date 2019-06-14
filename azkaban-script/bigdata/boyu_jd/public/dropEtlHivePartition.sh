#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql=`cat << EOF
use bigdata;

alter table bigdata.boyu_jd_goods_origin drop IF EXISTS partition(dt='${date}');
alter table bigdata.boyu_jd_goods_brand_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.boyu_jd_goods_day_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.boyu_jd_goods_mix_snapshot drop IF EXISTS partition(dt='${date}');
alter table bigdata.boyu_jd_goods_stats_snapshot drop IF EXISTS partition(dt='${date}');


EOF`

executeHiveCommand "${hive_sql}"