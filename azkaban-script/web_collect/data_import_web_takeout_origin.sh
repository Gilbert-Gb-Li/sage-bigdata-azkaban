#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2

sh -x ${GOBBLIN_HOME}/start-mr.sh web-takeout.pull ${GOBBLIN_HOME}

createPartitionDiff web.tbl_ex_takeout_shop_comment_detail_origin /data/web/origin/web-takeout-comment-detail-topic $date
createPartitionDiff web.tbl_ex_takeout_shop_comment_total_origin /data/web/origin/web-takeout-comment-total-topic $date
createPartitionDiff web.tbl_ex_takeout_shop_info_origin /data/web/origin/web-takeout-shop-info-topic $date
createPartitionDiff web.tbl_ex_takeout_shop_origin /data/web/origin/web-takeout-shop-topic $date