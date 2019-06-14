#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
hour=$2
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

createPartitionDiffHour bigdata.douyin_user_data_origin /data/douyin/origin/short-video-user $date $hour
createPartitionDiffHour bigdata.douyin_video_data_origin /data/douyin/origin/short-video-data $date $hour
createPartitionDiffHour bigdata.douyin_video_comment_data_origin /data/douyin/origin/short-video-comment $date $hour
createPartitionDiffHour bigdata.douyin_hot_music_data_origin /data/douyin/origin/hot_music_info $date $hour
createPartitionDiffHour bigdata.douyin_hot_challenge_data_origin /data/douyin/origin/hot_challenge_info $date $hour
createPartitionDiffHour bigdata.douyin_shop_window_data_origin /data/douyin/origin/shop_window $date $hour
createPartitionDiffHour bigdata.douyin_shop_window_goods_data_origin /data/douyin/origin/store_shop_list $date $hour
createPartitionDiffHour bigdata.douyin_hot_recommend_rank_data_origin /data/douyin/origin/hot_recommend_rankinng $date $hour
createPartitionDiffHour bigdata.douyin_hot_recommend_details_data_origin /data/douyin/origin/hot_recommend_details $date $hour
createPartitionDiffHour bigdata.douyin_attention_follower_data_origin /data/douyin/origin/attention_follower_list $date $hour
createPartitionDiffHour bigdata.douyin_challenge_data_origin /data/douyin/origin/challenge_info $date $hour
createPartitionDiffHour bigdata.douyin_music_data_origin /data/douyin/origin/music_info $date $hour
createPartitionDiffHour bigdata.douyin_hot_search_list_data_origin /data/douyin/origin/hot_search_list $date $hour
createPartitionDiffHour bigdata.douyin_hot_recommend_video_and_user_data_origin /data/douyin/origin/home_recommend_info $date $hour
createPartitionDiffHour bigdata.douyin_user_video_count_data_origin /data/douyin/origin/short-video-count $date $hour

if [ ${hour} -eq '00' ]
    then
      createPartitionDiff bigdata.douyin_video_voice_to_words_data_origin /data/douyin/origin/douyin_video_voice_to_words_data_origin $yesterday
    else
      echo "不是当天的00点，不进行导入视频转文字内容！"
fi

