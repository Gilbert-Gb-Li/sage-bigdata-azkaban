#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

day=$1

echo ${day}

echo "##############  hive表加载数据 start ##################"

    pinduoduo_boyu_goods_origin="alter table bigdata.pinduoduo_boyu_goods_origin add if not exists partition (dt='$day') location '/data/pinduoduo/origin/pinduoduo_boyu_goods_origin/$day'; "
    echo "##############添加分区   ${pinduoduo_boyu_goods_origin}    ###########"

    executeHiveCommand "
    ${pinduoduo_boyu_goods_origin}
    "

echo "##############   hive表加载数据 end  ##################"


