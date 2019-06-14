#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2

sh -x ${GOBBLIN_HOME}/start-mr.sh ias-news.pull ${GOBBLIN_HOME}

createPartitionDiffHour ias.tbl_ex_pengpai_news_data_origin /data/ias/origin/pengpai-news $date $hour
createPartitionDiffHour ias.tbl_ex_pengpai_news_comment_data_origin /data/ias/origin/pengpai-news-comment $date $hour
createPartitionDiffHour ias.tbl_ex_pengpai_topic_data_origin /data/ias/origin/pengpai-topic $date $hour
createPartitionDiffHour ias.tbl_ex_pengpai_topic_comment_data_origin /data/ias/origin/pengpai-topic-comment $date $hour