#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

value1=$1
value2=$2
if [ $value2 == 23 ]; then
    day=`date -d "1 day $value1" +%Y-%m-%d`
    hour=`date -d "1 hour $value2" +"%H"`
else
    day=$value1
    hour=`date -d "1 hour $value2" +"%H"`
fi

echo ${day} ${hour}

echo "##############  hive表加载数据 start ##################"

    kuaishou_user_data_origin=" alter table bigdata.kuaishou_user_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/kuaishou_user_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_user_data_origin}    ###########"

    kuaishou_talk_info_data_origin=" alter table bigdata.kuaishou_talk_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/short_talk_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_talk_info_data_origin}   ###########"

    kuaishou_short_video_data_origin=" alter table bigdata.kuaishou_short_video_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/short_video_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_short_video_data_origin}   ###########"

    kuaishou_short_video_comment_data_origin=" alter table bigdata.kuaishou_short_video_comment_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/video_comment_list/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_short_video_comment_data_origin}   ###########"

    kuaishou_music_data_origin=" alter table bigdata.kuaishou_music_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/music_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_music_data_origin}   ###########"

    kuaishou_location_video_info_data_origin=" alter table bigdata.kuaishou_location_video_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/location_video_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_location_video_info_data_origin}   ###########"

    kuaishou_live_gift_data_origin=" alter table bigdata.kuaishou_live_gift_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/live_gift_list/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_live_gift_data_origin}   ###########"

    kuaishou_live_end_data_origin=" alter table bigdata.kuaishou_live_end_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/live_end_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_live_end_data_origin}   ###########"

    kuaishou_live_danmu_data_origin=" alter table bigdata.kuaishou_live_danmu_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/kuaishou_live_danmu/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_live_danmu_data_origin}   ###########"

    kuaishou_challenge_video_info_data_origin=" alter table bigdata.kuaishou_challenge_video_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/challenge_video_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_challenge_video_info_data_origin}   ###########"

    kuaishou_user_commodity_info_data_origin=" alter table bigdata.kuaishou_user_commodity_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/commodity_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_user_commodity_info_data_origin}   ###########"

    kuaishou_live_user_info_data_origin=" alter table bigdata.kuaishou_live_user_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '/data/kuaishou/origin/live_user_info/$day/$hour'; "
    echo "##############添加分区   ${kuaishou_live_user_info_data_origin}   ###########"



    executeHiveCommand "
    ${kuaishou_user_data_origin}
    ${kuaishou_talk_info_data_origin}
    ${kuaishou_short_video_data_origin}
    ${kuaishou_short_video_comment_data_origin}
    ${kuaishou_music_data_origin}
    ${kuaishou_location_video_info_data_origin}
    ${kuaishou_live_gift_data_origin}
    ${kuaishou_live_end_data_origin}
    ${kuaishou_live_danmu_data_origin}
    ${kuaishou_challenge_video_info_data_origin}
    ${kuaishou_user_commodity_info_data_origin}
    ${kuaishou_live_user_info_data_origin}
    "

echo "##############   hive表加载数据 end  ##################"


