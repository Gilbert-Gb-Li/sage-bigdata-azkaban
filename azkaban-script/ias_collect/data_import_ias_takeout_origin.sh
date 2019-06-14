#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2

sh -x ${GOBBLIN_HOME}/start-mr.sh ias-takeout.pull ${GOBBLIN_HOME}

createPartitionDiff ias.tbl_ex_takeout_shop_comment_detail_origin /data/ias/origin/ias-takeout-comment-detail-topic $date
createPartitionDiff ias.tbl_ex_takeout_shop_comment_total_origin /data/ias/origin/ias-takeout-comment-total-topic $date
createPartitionDiff ias.tbl_ex_takeout_shop_info_origin /data/ias/origin/ias-takeout-shop-info-topic $date
createPartitionDiff ias.tbl_ex_takeout_shop_license_origin /data/ias/origin/takeout-license-topic $date
createPartitionDiff ias.tbl_ex_takeout_shop_origin /data/ias/origin/ias-takeout-shop-topic $date