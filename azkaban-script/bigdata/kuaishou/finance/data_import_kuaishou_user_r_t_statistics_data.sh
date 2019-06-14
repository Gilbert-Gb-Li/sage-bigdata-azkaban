#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
stat_date=`date -d "$date" +%Y%m%d`
month1=`date -d "${date}" +%Y-%m`
month1_01=`date -d "${month1}-01" +%Y-%m-%d`
month1_01_1=`date -d "${month1}-01" +%Y%m%d`
month1_01_yesterday_1=`date -d "-1 day $month1_01" +%Y-%m-%d`
month2=`date -d "${month1_01_yesterday_1}" +%Y-%m`
month2_01=`date -d "${month2}-01" +%Y-%m-%d`
month2_01_1=`date -d "${month2}-01" +%Y%m%d`
year=`date -d "$date" +%Y`
month=`date -d "$date" +%m`
echo "date:${date}"
echo "yesterday:${yesterday}"
echo "stat_date:${stat_date}"
echo "month1:${month1}"
echo "month1_01:${month1_01}"
echo "month1_01_1:${month1_01_1}"
echo "month1_01_yesterday_1:${month1_01_yesterday_1}"
echo "month2:${month2}"
echo "month2_01:${month2_01}"
echo "month2_01_1:${month2_01_1}"
echo "year:${year}"
echo "month:${month}"

apk_name="com.smile.gifmaker"

    echo "##################### 统计头部抽样用户数据 #############################"
    kuaishou_user_r_t_statistics_data_save_hive="
    insert into table bigdata.kuaishou_user_r_t_statistics_data partition(dt='${date}')
    SELECT
        a.set_type,
        concat('${stat_date}',a.set_type,'r_t_statistics_data') as keyWord,
        '${year}${month}' AS stat_month,
        'kuaishoui' as meta_app_name,
        'kuaishou_user_r_t_statistics_data' as meta_table_name,
        a.app_package_name,
        a.extract_date,
        a.user_count,
        if(b.user_new_short_video_count >0,b.user_new_short_video_count,0) as user_new_short_video_count,
        if(c.user_new_challenge_video_count >0,c.user_new_challenge_video_count,0) as user_new_challenge_video_count,
        if(e.user_store_count >0,e.user_store_count,0) as user_store_count,
        if(e2.user_curriculum_count >0,e2.user_curriculum_count,0) as user_curriculum_count,
        if(f.user_commodity_count >0,f.user_commodity_count,0) as user_commodity_count,
        if(h.user_new_talk_count >0,h.user_new_talk_count,0) as user_new_talk_count,
        if(i.user_new_music_count >0,i.user_new_music_count,0) as user_new_music_count,
        if(j.user_new_like_count >0,j.user_new_like_count,0) as user_new_like_count,
        if(k.user_new_video_comment_count >0,k.user_new_video_comment_count,0) as user_new_video_comment_count,
        if(l.user_new_user_comment_count >0,l.user_new_user_comment_count,0) as user_new_user_comment_count,
        if(m.user_new_talk_comment_count >0,m.user_new_talk_comment_count,0) as user_new_talk_comment_count,
        if(n.user_new_follower_count >0,n.user_new_follower_count,0) as user_new_follower_count,
        if(d.user_new_following_count >0,d.user_new_following_count,0) as user_new_following_count,

        if(b.user_new_short_video_count >0,b.user_new_short_video_count,0)/a.user_count*100 as proportion_new_short_video_count,
        if(c.user_new_challenge_video_count >0,c.user_new_challenge_video_count,0)/a.user_count*100 as proportion_new_challenge_video_count,
        if(e.user_store_count >0,e.user_store_count,0)/a.user_count*100 as proportion_store_count,
        if(e2.user_curriculum_count >0,e2.user_curriculum_count,0)/a.user_count*100 as proportion_curriculum_count,
        if(f.user_commodity_count >0,f.user_commodity_count,0)/a.user_count*100 as proportion_commodity_count,
        if(h.user_new_talk_count >0,h.user_new_talk_count,0)/a.user_count*100 as proportion_new_talk_count,
        if(i.user_new_music_count >0,i.user_new_music_count,0)/a.user_count*100 as proportion_new_music_count,
        if(j.user_new_like_count >0,j.user_new_like_count,0)/a.user_count*100 as proportion_new_like_count,
        if(k.user_new_video_comment_count >0,k.user_new_video_comment_count,0)/a.user_count*100 as proportion_new_video_comment_count,
        if(l.user_new_user_comment_count >0,l.user_new_user_comment_count,0)/a.user_count*100 as proportion_new_user_comment_count,
        if(m.user_new_talk_comment_count >0,m.user_new_talk_comment_count,0)/a.user_count*100 as proportion_new_talk_comment_count,
        if(n.user_new_follower_count >0,n.user_new_follower_count,0)/a.user_count*100 as proportion_new_follower_count,
        if(d.user_new_following_count >0,d.user_new_following_count,0)/a.user_count*100 as proportion_new_following_count
    from(
        SELECT '${apk_name}' as app_package_name,'${stat_date}' as extract_date,'T_USER' as set_type,COUNT(distinct user_id) AS user_count
        FROM bigdata.kuaishou_header_user_data_orc
        WHERE dt = '${date}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,'${month1_01_1}' as extract_date,'R_USER' as set_type,COUNT(distinct user_id) AS user_count
        FROM bigdata.kuaishou_sampling_user_data_orc
        WHERE dt = '${month1_01}'
            AND app_package_name IN ('${apk_name}')
        UNION ALL
        SELECT '${apk_name}' as app_package_name,'${month2_01_1}' as extract_date,'R_USER' as set_type,COUNT(distinct user_id) AS user_count
        FROM bigdata.kuaishou_sampling_user_data_orc
        WHERE dt = '${month2_01}'
            AND app_package_name IN ('${apk_name}')
    ) as a
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_short_video_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_short_video_count>0
        group by app_package_name,extract_date,set_type
    ) as b
    on a.app_package_name=b.app_package_name and a.extract_date=b.extract_date and a.set_type=b.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_challenge_video_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_challenge_video_count>0
        group by app_package_name,extract_date,set_type
    ) as c
    on a.app_package_name=c.app_package_name and a.extract_date=c.extract_date and a.set_type=c.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_following_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_following_count>0
        group by app_package_name,extract_date,set_type
    ) as d
    on a.app_package_name=d.app_package_name and a.extract_date=d.extract_date and a.set_type=d.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_store_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and store_or_curriculum=1
        group by app_package_name,extract_date,set_type
    ) as e
    on a.app_package_name=e.app_package_name and a.extract_date=e.extract_date and a.set_type=e.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_curriculum_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and store_or_curriculum=2
        group by app_package_name,extract_date,set_type
    ) as e2
    on a.app_package_name=e2.app_package_name and a.extract_date=e2.extract_date and a.set_type=e2.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_commodity_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and commodity_count>0
        group by app_package_name,extract_date,set_type
    ) as f
    on a.app_package_name=f.app_package_name and a.extract_date=f.extract_date and a.set_type=f.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_talk_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_talk_count>0
        group by app_package_name,extract_date,set_type
    ) as h
    on a.app_package_name=h.app_package_name and a.extract_date=h.extract_date and a.set_type=h.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_music_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_music_count>0
        group by app_package_name,extract_date,set_type
    ) as i
    on a.app_package_name=i.app_package_name and a.extract_date=i.extract_date and a.set_type=i.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_like_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_like_count>0
        group by app_package_name,extract_date,set_type
    ) as j
    on a.app_package_name=j.app_package_name and a.extract_date=j.extract_date and a.set_type=j.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_video_comment_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_video_comment_count>0
        group by app_package_name,extract_date,set_type
    ) as k
    on a.app_package_name=k.app_package_name and a.extract_date=k.extract_date and a.set_type=k.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_user_comment_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_user_comment_count>0
        group by app_package_name,extract_date,set_type
    ) as l
    on a.app_package_name=l.app_package_name and a.extract_date=l.extract_date and a.set_type=l.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_talk_comment_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_talk_comment_count>0
        group by app_package_name,extract_date,set_type
    ) as m
    on a.app_package_name=m.app_package_name and a.extract_date=m.extract_date and a.set_type=m.set_type
    left join(
        SELECT app_package_name,extract_date,set_type, COUNT(distinct user_id) AS user_new_follower_count
        FROM bigdata.kuaishou_user_r_t_data
        WHERE dt = '${date}' AND app_package_name IN ('${apk_name}')
            and new_follower_count>0
        group by app_package_name,extract_date,set_type
    ) as n
    on a.app_package_name=n.app_package_name and a.extract_date=n.extract_date and a.set_type=n.set_type
    ;
    "
    echo "${kuaishou_user_r_t_statistics_data_save_hive}"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_user_r_t_statistics_data DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_user_r_t_statistics_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${kuaishou_user_r_t_statistics_data_save_hive}
    "
