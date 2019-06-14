#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

createPartitionDiff bigdata.douyin_advert_brand_data_origin /data/douyin/advert_snapshot/douyin_advert_brand_data_origin $date 
createPartitionDiff bigdata.douyin_advert_category_data_origin /data/douyin/advert_snapshot/douyin_advert_category_data_origin $date 