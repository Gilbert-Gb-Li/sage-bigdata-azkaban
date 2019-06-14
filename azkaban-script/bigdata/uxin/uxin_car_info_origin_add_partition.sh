#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='00') location '/data/uxin/origin/car_info/$date/00';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='01') location '/data/uxin/origin/car_info/$date/01';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='02') location '/data/uxin/origin/car_info/$date/02';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='03') location '/data/uxin/origin/car_info/$date/03';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='04') location '/data/uxin/origin/car_info/$date/04';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='05') location '/data/uxin/origin/car_info/$date/05';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='06') location '/data/uxin/origin/car_info/$date/06';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='07') location '/data/uxin/origin/car_info/$date/07';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='08') location '/data/uxin/origin/car_info/$date/08';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='09') location '/data/uxin/origin/car_info/$date/09';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='10') location '/data/uxin/origin/car_info/$date/10';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='11') location '/data/uxin/origin/car_info/$date/11';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='12') location '/data/uxin/origin/car_info/$date/12';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='13') location '/data/uxin/origin/car_info/$date/13';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='14') location '/data/uxin/origin/car_info/$date/14';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='15') location '/data/uxin/origin/car_info/$date/15';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='16') location '/data/uxin/origin/car_info/$date/16';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='17') location '/data/uxin/origin/car_info/$date/17';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='18') location '/data/uxin/origin/car_info/$date/18';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='19') location '/data/uxin/origin/car_info/$date/19';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='20') location '/data/uxin/origin/car_info/$date/20';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='21') location '/data/uxin/origin/car_info/$date/21';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='22') location '/data/uxin/origin/car_info/$date/22';
alter table bigdata.uxin_car_info_origin add IF NOT EXISTS partition (dt='$date', hour='23') location '/data/uxin/origin/car_info/$date/23';
"

executeHiveCommand "${hive_sql}"
