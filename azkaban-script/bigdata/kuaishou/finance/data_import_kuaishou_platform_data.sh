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


    echo '################平台信息统计 ####################'
    kuaishou_platform_data_save_hive="
    insert into table bigdata.kuaishou_platform_data partition(dt='${date}')
    SELECT  concat('${stat_date}','_kuaishou_platform_data') as keyWord,
        'kuaishoui' as meta_app_name,
        'kuaishou_platform_data' as meta_table_name,
        a.app_package_name,
        a.user_total_count,
        a.user_total_count-b.yesterday_user_total_count as new_user_count,
        c.video_total_count,
        c.video_total_count-d.yesterday_video_total_count as new_video_count,
        e.video_comment_total_count,
        e.video_comment_total_count-e2.yesterday_video_comment_total_count as new_video_comment_total_count,
        f.talk_total_count,h.talk_comment_total_count,i.music_total_count,
        j.challenge_total_count,
        if(k.live_active_user_count >0,k.live_active_user_count,0) as live_active_user_count,
        if(l.live_gift_total_money >0,l.live_gift_total_money,0) as live_gift_total_money,
        if(m.live_pay_user_count >0,m.live_pay_user_count,0) as live_pay_user_count,
        n.t_user_count,
        n.t_user_count-o.yesterday_t_user_count as new_t_user_count
    from(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS user_total_count
        FROM bigdata.kuaishou_user_data_daily_snapshot
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as a
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS yesterday_user_total_count
        FROM bigdata.kuaishou_user_data_daily_snapshot
        WHERE dt = '${yesterday}'
            AND app_package_name IN ('${apk_name}')
    ) as b
    on a.app_package_name=b.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS video_total_count
        FROM bigdata.kuaishou_short_video_data_daily_snapshot
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as c
    on a.app_package_name=c.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS yesterday_video_total_count
        FROM bigdata.kuaishou_short_video_data_daily_snapshot
        WHERE dt = '${yesterday}'
            AND app_package_name IN ('${apk_name}')
    ) as d
    on a.app_package_name=d.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS video_comment_total_count
        FROM bigdata.kuaishou_short_video_comment_data_daily_snapshot
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as e
    on a.app_package_name=e.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS yesterday_video_comment_total_count
        FROM bigdata.kuaishou_short_video_comment_data_daily_snapshot
        WHERE dt = '${yesterday}'
            AND app_package_name IN ('${apk_name}')
    ) as e2
    on a.app_package_name=e2.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(distinct talk_id) AS talk_total_count
        FROM bigdata.kuaishou_talk_comment_data_daily_snapshot
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as f
    on a.app_package_name=f.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS talk_comment_total_count
        FROM bigdata.kuaishou_talk_comment_data_daily_snapshot
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as h
    on a.app_package_name=h.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS music_total_count
        FROM bigdata.kuaishou_music_data_daily_snapshot
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as i
    on a.app_package_name=i.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(distinct challenge_from) AS challenge_total_count
        FROM bigdata.kuaishou_challenge_video_data_daily_snapshot
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as j
    on a.app_package_name=j.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS live_active_user_count
        FROM bigdata.kuaishou_live_user_all_data
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as k
    on a.app_package_name=k.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, sum(gift_val) AS live_gift_total_money
        FROM bigdata.kuaishou_live_danmu_gift_data
        WHERE dt = '${date}'
            AND gift_val>0
            AND app_package_name IN ('${apk_name}')
    ) as l
    on a.app_package_name=l.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(distinct user_id) AS live_pay_user_count
        FROM bigdata.kuaishou_live_audience_all_data
        WHERE dt = '${date}'
            AND receive_gift_val>0
            AND app_package_name IN ('${apk_name}')
    ) as m
    on a.app_package_name=m.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS t_user_count
        FROM bigdata.kuaishou_header_user_data_orc
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
    ) as n
    on a.app_package_name=n.app_package_name
    left join(
        SELECT '${apk_name}' as app_package_name, COUNT(1) AS yesterday_t_user_count
        FROM bigdata.kuaishou_header_user_data_orc
        WHERE dt = '${yesterday}'
            AND app_package_name IN ('${apk_name}')
    ) as o
    on a.app_package_name=o.app_package_name
    ;
    "
    echo "${kuaishou_platform_data_save_hive}"



    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_platform_data DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_platform_data/dt=${date}


    executeHiveCommand "
    ${delete_hive_partitions}
    ${kuaishou_platform_data_save_hive}
    "

