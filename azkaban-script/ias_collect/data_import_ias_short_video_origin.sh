#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

sh -x ${GOBBLIN_HOME}/start-mr.sh ias-shortvideo.pull ${GOBBLIN_HOME}

createPartitionDiff ias.tbl_ex_short_video_user_origin /data/ias/origin/short-video-user $date
createPartitionDiff ias.tbl_ex_short_video_data_origin /data/ias/origin/short-video-data $date
createPartitionDiff ias.tbl_ex_short_video_comment_origin /data/ias/origin/short-video-comment $date
createPartitionDiff ias.tbl_ex_short_video_music_data_origin /data/ias/origin/short-video-music $date
createPartitionDiff ias.tbl_ex_short_video_user_like_link_origin /data/ias/origin/short-video-user-like-link $date
createPartitionDiff ias.tbl_ex_short_video_challenge_data_origin /data/ias/origin/short-video-challenge $date
createPartitionDiff ias.tbl_ex_short_video_user_detail_origin /data/ias/origin/short-video-user-detail $date