#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2

sh -x ${GOBBLIN_HOME}/start-mr.sh ias-weibo.pull ${GOBBLIN_HOME}

createPartitionDiffHour ias.tbl_ex_weibo_user_topic_data_origin /data/ias/origin/weibo-user-topic $date $hour
createPartitionDiffHour ias.tbl_ex_weibo_article_topic_data_origin /data/ias/origin/weibo-article-topic $date $hour
createPartitionDiffHour ias.tbl_ex_weibo_comment_topic_data_origin /data/ias/origin/weibo-comment-topic $date $hour
createPartitionDiffHour ias.tbl_ex_weibo_forward_topic_data_origin /data/ias/origin/weibo-forward-topic $date $hour