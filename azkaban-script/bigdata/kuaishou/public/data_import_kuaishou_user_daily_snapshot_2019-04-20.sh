#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

    echo "####################### 快手全量用户信息快照 #################################"
    tmp_kuaishou_all_user_data="
    insert into bigdata.kuaishou_user_data_daily_snapshot partition(dt='${date}')
    select e.data_generate_time,e.app_package_name,e.user_name,e.user_id,f.kwai_id,
        e.user_share_url,e.follower_count,e.following_count,e.signature,
        e.store_or_curriculum,e.curriculum,e.sex,e.constellation,e.certification,e.short_video_count,e.talk_count,e.music_count,e.label3,
        u. user_avatar_url,
        e.app_version,e.resource_key
    from (
        select d.data_generate_time,d.app_package_name,d.user_name,d.user_id,d.kwai_id,d.user_share_url,d.follower_count,d.following_count,d.signature,
            d.store_or_curriculum,d.curriculum,d.sex,d.constellation,d.certification,d.short_video_count,d.talk_count,d.music_count,d.label3,
            d.user_avatar_url,d.app_version,d.resource_key
            from(
                select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                from(
                    select
                        data_generate_time,app_package_name,user_name,user_id,null as kwai_id,user_share_url,follower_count,following_count,signature,store_or_curriculum,curriculum,sex,constellation,certification,short_video_count,talk_count,music_count,label3, null as user_avatar_url,app_version,resource_key
                    from  bigdata.kuaishou_user_data_origin_orc
                    where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!=''
                    UNION ALL
                    select
                        data_generate_time,app_package_name,user_name,user_id,kwai_id,user_share_url,follower_count,following_count,signature,store_or_curriculum,curriculum,sex,constellation,certification,short_video_count,talk_count,music_count,label3,user_avatar_url,app_version,resource_key
                    from bigdata.kuaishou_user_data_daily_snapshot
                    where dt='${yesterday}'
                ) as c
        ) as d
        where d.order_num=1
    ) as e
    left join(
            select d.app_package_name,d.user_id,d.kwai_id
            from(
                select c.data_generate_time,c.app_package_name,c.user_id,c.kwai_id,row_number() over (partition by c.app_package_name,c.user_id order by c.data_generate_time desc) as order_num
                from(
                    select
                        data_generate_time,app_package_name,user_id,kwai_id
                    from(
                        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                        from bigdata.kuaishou_short_video_data_origin_orc
                        where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
                    ) as a
                    where a.order_num=1
                    UNION ALL
                    select
                        data_generate_time,app_package_name,user_id,kwai_id
                    from(
                        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                        from bigdata.kuaishou_location_video_info_data_origin_orc
                        where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
                    ) as a
                    where a.order_num=1
                    UNION ALL
                    select
                        data_generate_time,app_package_name,user_id,kwai_id
                    from(
                        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                        from bigdata.kuaishou_challenge_video_info_data_origin_orc
                        where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
                    ) as a
                    where a.order_num=1
                    UNION ALL
                    select
                        data_generate_time,app_package_name,user_id,kwai_id
                    from bigdata.kuaishou_user_data_daily_snapshot
                    where dt='${yesterday}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
                ) as c
            ) as d
            where d.order_num=1
    ) as f
    on e.app_package_name=f.app_package_name and e.user_id=f.user_id
    left join(
            select d.app_package_name,d.user_id,d.user_avatar_url
            from(
                select c.data_generate_time,c.app_package_name,c.user_id,c.user_avatar_url,row_number() over (partition by c.app_package_name,c.user_id order by c.data_generate_time desc) as order_num
                from(
                    select
                        data_generate_time,app_package_name,user_id,user_avatar_url
                    from(
                        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                        from bigdata.kuaishou_short_video_data_origin_orc
                        where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and user_avatar_url is not null and user_avatar_url!=''
                    ) as a
                    where a.order_num=1
                    UNION ALL
                    select
                        data_generate_time,app_package_name,user_id,user_avatar_url
                    from(
                        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                        from bigdata.kuaishou_location_video_info_data_origin_orc
                        where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and user_avatar_url is not null and user_avatar_url!=''
                    ) as a
                    where a.order_num=1
                    UNION ALL
                    select
                        data_generate_time,app_package_name,user_id,user_avatar_url
                    from(
                        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                        from bigdata.kuaishou_challenge_video_info_data_origin_orc
                        where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and user_avatar_url is not null and user_avatar_url!=''
                    ) as a
                    where a.order_num=1
                    UNION ALL
                    select
                        data_generate_time,app_package_name,user_id,user_avatar_url
                    from bigdata.kuaishou_user_data_daily_snapshot
                    where dt='${yesterday}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and user_avatar_url is not null and user_avatar_url!=''
                ) as c
            ) as d
            where d.order_num=1
    ) as u
    on e.app_package_name=u.app_package_name and e.user_id=u.user_id
    ;
    "
    echo "${tmp_kuaishou_all_user_data}"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_user_data_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_user_data_daily_snapshot/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_kuaishou_all_user_data}"