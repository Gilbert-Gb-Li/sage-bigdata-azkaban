#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh


date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
stat_date=`date -d "$date" +%Y%m%d`
year=`date -d "$date" +%Y`
month=`date -d "$date" +%m`
echo "date:${date}"
echo "yesterday:${yesterday}"
echo "stat_date:${stat_date}"
echo "year:${year}"
echo "month:${month}"

apk_name="com.smile.gifmaker"


    echo '################获取快手原始数据量 ####################'
    tmp_kuaishou_origin_count="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_origin_count AS
    select '${date}' as dt,a.app_package_name,
        (max(a.user_data_count) +
        max(a.user_commodity_data_count) +
        max(a.user_talk_data_count) +
        max(a.video_data_count) +
        max(a.video_comment_data_count) +
        max(a.music_data_count) +
        max(a.location_video_data_count) +
        max(a.live_user_data_count) +
        max(a.live_gift_data_count) +
        max(a.live_end_data_count) +
        max(a.live_danmu_data_count) +
        max(a.challenge_video_data_count)) as origin_data_count,
        max(a.user_data_count) as user_data_count,
        max(a.user_commodity_data_count) as user_commodity_data_count,
        max(a.user_talk_data_count) as user_talk_data_count,
        max(a.video_data_count) as video_data_count,
        max(a.video_comment_data_count) as video_comment_data_count,
        max(a.music_data_count) as music_data_count,
        max(a.location_video_data_count) as location_video_data_count,
        max(a.live_user_data_count) as live_user_data_count,
        max(a.live_gift_data_count) as live_gift_data_count,
        max(a.live_end_data_count) as live_end_data_count,
        max(a.live_danmu_data_count) as live_danmu_data_count,
        max(a.challenge_video_data_count) as challenge_video_data_count
    from (
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_user_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,count(1) as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_user_commodity_info_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,count(1) as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_talk_info_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            count(1) as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_short_video_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,count(1) as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_short_video_comment_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,count(1) as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_music_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,count(1) as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_location_video_info_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,count(1) as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_user_info_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            count(1) as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_gift_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,count(1) as live_end_data_count,0 as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_end_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,count(1) as live_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_danmu_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,count(1) as challenge_video_data_count
        FROM bigdata.kuaishou_challenge_video_info_data_origin
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as a
    group by a.app_package_name
    ;
    "
    echo "${tmp_kuaishou_origin_count}"

    echo '################获取快手可用原始数据量 ####################'
    tmp_kuaishou_valid_origin_count="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_valid_origin_count AS
    select '${date}' as dt,a.app_package_name,
        (max(a.user_data_count) +
        max(a.user_commodity_data_count) +
        max(a.user_talk_data_count) +
        max(a.video_data_count) +
        max(a.video_comment_data_count) +
        max(a.music_data_count) +
        max(a.location_video_data_count) +
        max(a.live_user_data_count) +
        max(a.live_gift_data_count) +
        max(a.live_end_data_count) +
        max(a.live_danmu_data_count) +
        max(a.challenge_video_data_count)) as valid_origin_data_count,
        max(a.user_data_count) as valid_user_data_count,
        max(a.user_commodity_data_count) as valid_user_commodity_data_count,
        max(a.user_talk_data_count) as valid_user_talk_data_count,
        max(a.video_data_count) as valid_video_data_count,
        max(a.video_comment_data_count) as valid_video_comment_data_count,
        max(a.music_data_count) as valid_music_data_count,
        max(a.location_video_data_count) as valid_location_video_data_count,
        max(a.live_user_data_count) as valid_live_user_data_count,
        max(a.live_gift_data_count) as valid_live_gift_data_count,
        max(a.live_end_data_count) as valid_live_end_data_count,
        max(a.live_danmu_data_count) as valid_live_danmu_data_count,
        max(a.live_gift_danmu_data_count) as valid_live_gift_danmu_data_count,
        max(a.challenge_video_data_count) as valid_challenge_video_data_count
    from (
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_user_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND user_id is not null AND user_id !=''
            AND user_name is not null AND user_name !=''
            AND (follower_count!=-1 or following_count!=-1 or short_video_count!=-1 or talk_count!=-1 or music_count!=-1)
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,count(1) as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_user_commodity_info_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND user_id is not null AND user_id !=''
            AND commodity_name is not null AND commodity_name !=''
            AND commodity_price>0
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,count(1) as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_talk_info_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND talk_id is not null AND talk_id !=''
            AND like_count >=0
            AND user_id is not null AND user_id !=''
            AND comment_id is not null AND comment_id !=''
            AND comment_user_id is not null AND comment_user_id !=''
            AND comment_count >=0
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            count(1) as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_short_video_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND video_id is not null AND video_id !=''
            AND user_id is not null AND user_id !=''
            AND video_like_count >=0
            AND video_play_count >=0
            AND video_comment_count >=0
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,count(1) as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_short_video_comment_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND comment_id is not null AND comment_id !=''
            AND video_id is not null AND video_id !=''
            AND video_user_id is not null AND video_user_id !=''
            AND comment_content is not null AND comment_content !=''
            AND comment_user_id is not null AND comment_user_id !=''
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,count(1) as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_music_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND music_id is not null AND music_id !=''
            AND music_name is not null AND music_name !=''
            AND musician_id is not null AND musician_id !=''
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,count(1) as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_location_video_info_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND video_id is not null AND video_id !=''
            AND user_id is not null AND user_id !=''
            AND video_like_count >=0
            AND video_play_count >=0
            AND video_comment_count >=0
            AND location_id is not null AND location_id !=''
            AND address_name is not null AND address_name !=''
            AND address_count >=0
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,count(1) as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_user_info_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND user_name is not null AND user_name !=''
            AND user_id is not null AND user_id !=''
            AND room_id is not null AND room_id !=''
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            count(1) as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_gift_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND user_id is not null AND user_id !=''
            AND gift_id is not null AND gift_id !=''
            AND gift_name is not null AND gift_name !=''
            AND gift_currency>=0
            AND gift_unit_val>=0
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,count(1) as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_end_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND user_id is not null AND user_id !=''
            AND audience_count>=0
            AND like_count>=0
            AND live_duration>=0
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,count(1) as live_danmu_data_count,0 as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_danmu_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND user_id is not null AND user_id !=''
            AND audience_id is not null AND audience_id !=''
            AND ((danmu_content is not null AND danmu_content !='') or (danmu_gift_id is not null AND danmu_gift_id !=''))
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name, 0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,count(1) as live_gift_danmu_data_count,0 as challenge_video_data_count
        FROM bigdata.kuaishou_live_danmu_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND user_id is not null AND user_id !=''
            AND audience_id is not null AND audience_id !=''
            AND danmu_gift_id is not null AND danmu_gift_id !=''
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,0 AS user_data_count,0 as user_commodity_data_count,0 as user_talk_data_count,
            0 as video_data_count,0 as video_comment_data_count,0 as music_data_count,0 as location_video_data_count,0 as live_user_data_count,
            0 as live_gift_data_count,0 as live_end_data_count,0 as live_danmu_data_count,0 as live_gift_danmu_data_count,count(1) as challenge_video_data_count
        FROM bigdata.kuaishou_challenge_video_info_data_origin_orc
        WHERE dt = '${date}'
            and data_generate_time is not null and data_generate_time>0
            AND video_id is not null AND video_id !=''
            AND user_id is not null AND user_id !=''
            AND video_like_count >=0
            AND video_play_count >=0
            AND video_comment_count >=0
            AND challenge_from is not null AND challenge_from !=''
            AND app_package_name IN ('${apk_name}')
    ) as a
    group by a.app_package_name
    ;
    "

    echo "${tmp_kuaishou_valid_origin_count}"


    echo '################信息存储hive####################'
    kuaishou_statistics_data_save_hive="
    insert into table bigdata.kuaishou_statistics_data partition(dt='${date}')
    SELECT  concat('${stat_date}','_kuaishou_statistics_data') as keyWord,
            'kuaishoui' as meta_app_name,
            'kuaishou_statistics_data' as meta_table_name,
            COALESCE(a.app_package_name,b.app_package_name) as app_package_name,
            a.origin_data_count  ,
            b.valid_origin_data_count  ,
            a.user_data_count  ,
            b.valid_user_data_count  ,
            a.user_commodity_data_count,
            b.valid_user_commodity_data_count  ,
            a.user_talk_data_count  ,
            b.valid_user_talk_data_count  ,
            a.video_data_count  ,
            b.valid_video_data_count  ,
            a.video_comment_data_count  ,
            b.valid_video_comment_data_count  ,
            a.music_data_count  ,
            b.valid_music_data_count  ,
            a.location_video_data_count  ,
            b.valid_location_video_data_count  ,
            a.live_user_data_count  ,
            b.valid_live_user_data_count  ,
            a.live_gift_data_count  ,
            b.valid_live_gift_data_count  ,
            a.live_end_data_count  ,
            b.valid_live_end_data_count  ,
            a.live_danmu_data_count  ,
            b.valid_live_danmu_data_count  ,
            b.valid_live_gift_danmu_data_count  ,
            a.challenge_video_data_count  ,
            b.valid_challenge_video_data_count
    FROM(
        SELECT * FROM DEFAULT.tmp_kuaishou_origin_count
    ) as a
    JOIN(
        SELECT * FROM DEFAULT.tmp_kuaishou_valid_origin_count
    ) as b
    on a.dt=b.dt and a.app_package_name=b.app_package_name
    ;
    "
    echo "${kuaishou_statistics_data_save_hive}"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_statistics_data DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_statistics_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_kuaishou_origin_count}
    ${tmp_kuaishou_valid_origin_count}
    ${kuaishou_statistics_data_save_hive}
    "

