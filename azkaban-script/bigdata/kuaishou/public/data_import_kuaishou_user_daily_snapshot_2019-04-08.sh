#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh



#
#  说明：4月17号之前上报的用户数据中的user_id可能是用户ID:345345 或者是快手ID:haohaio8888
#  导致在全量用户中同一用户有用户ID和快手ID，并且这两个id不一样，在统计全量用户时计两次。
#  这样的用户大概有300万左右。所有才写此脚本进行去重。
#  从4月19号之后用户数据中上报的user_id都是用户ID了，不需要这么复杂的去重逻辑，影响计算时间。
#  但是，粉丝小于8000的user_id是不更新的，所以还的继续使用该方法。等用户足够多的时候，就会全部替换掉user_id是快手号的用户。
#


date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

apk_name="com.smile.gifmaker"

    echo "#####################  每日user_id和快手号start ########################"
    tmp_kuaishou_user_kwai_id_data="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_user_kwai_id_data AS
    select *
    from(
        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
        from(
            select
                data_generate_time,app_package_name,user_id,kwai_id
            from(
                select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                from bigdata.kuaishou_short_video_data_origin_orc
                where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
            ) as a
            where a.order_num=1
            UNION
            select
                data_generate_time,app_package_name,user_id,kwai_id
            from(
                select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                from bigdata.kuaishou_location_video_info_data_origin_orc
                where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
            ) as a
            where a.order_num=1
            UNION
            select
                data_generate_time,app_package_name,user_id,kwai_id
            from(
                select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
                from bigdata.kuaishou_challenge_video_info_data_origin_orc
                where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
            ) as a
            where a.order_num=1
            UNION
            select
                data_generate_time,app_package_name,user_id,kwai_id
            from bigdata.kuaishou_user_data_daily_snapshot
            where dt='${yesterday}' and app_package_name='${apk_name}' and user_id is not null and user_id!='' and kwai_id is not null and kwai_id!=''
        ) as c
    ) as d
    where d.order_num=1;
    "
    echo "${tmp_kuaishou_user_kwai_id_data}"
    echo "#############################################################"

    echo "#####################  每日快手用户头像连接  #######################"
    tmp_kuaishou_user_avatar_url_data="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_user_avatar_url_data AS
    select *
    from(
        select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
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
    where d.order_num=1;
    "
    echo "${tmp_kuaishou_user_avatar_url_data}"
    echo "#############################################################"

    echo "#########################  全量user_id，快手号，resource_key  数据####################################"
    tmp_kuaishou_all_user_id_kwai_id_resource_key_data="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_all_user_id_kwai_id_resource_key_data AS
    select e.data_generate_time,e.user_id,
        y.kwai_id,
        e.resource_key
    from (
        select d.data_generate_time,d.user_id,d.kwai_id,d.resource_key
        from(
            select c.data_generate_time,c.user_id,c.kwai_id,c.resource_key,
                row_number() over (partition by c.user_id order by c.data_generate_time desc) as order_num
            from(
                select
                    data_generate_time,user_id,null as kwai_id,resource_key
                from  bigdata.kuaishou_user_data_origin_orc
                where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!=''
                UNION ALL
                select
                    data_generate_time,user_id,kwai_id,resource_key
                from bigdata.kuaishou_user_data_daily_snapshot
                where dt='${yesterday}'
            ) as c
        ) as d
        where d.order_num=1
    ) as e
    left join(
        select user_id,kwai_id from default.tmp_kuaishou_user_kwai_id_data
    ) as y
    on e.user_id=y.user_id;
    "
    echo "${tmp_kuaishou_all_user_id_kwai_id_resource_key_data}"
    echo "#############################################################"

    echo "######################### 删除部分快手号和user_id相同后的数据####################################"
    tmp_kuaishou_user_id_kwai_id_resource_key_data_set="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_user_id_kwai_id_resource_key_data_set AS
    select a.data_generate_time,a.user_id,a.kwai_id,a.resource_key
    from(
        select data_generate_time,user_id,kwai_id,resource_key
        from default.tmp_kuaishou_all_user_id_kwai_id_resource_key_data
    ) as a
    left join(
        select distinct kwai_id from default.tmp_kuaishou_all_user_id_kwai_id_resource_key_data where kwai_id is not null and kwai_id!=''
    ) as b
    on a.user_id=b.kwai_id
    where b.kwai_id is null;
    "
    ${tmp_kuaishou_user_id_kwai_id_resource_key_data_set}
    echo "#############################################################"

    echo "######################### 全量用户user_id数据####################################"
    tmp_kuaishou_all_user_id_set="
    CREATE TEMPORARY TABLE default.tmp_kuaishou_all_user_id_set AS
    select f.data_generate_time,f.user_id,f.kwai_id,f.resource_key
    from(
        select b.data_generate_time,b.user_id,b.kwai_id,b.resource_key
        from(
            select data_generate_time,user_id,kwai_id,resource_key
            from default.tmp_kuaishou_user_id_kwai_id_resource_key_data_set
        ) as b
        left join(
            select a.resource_key
            from(
                select resource_key,count(1) as num
                from default.tmp_kuaishou_user_id_kwai_id_resource_key_data_set
                group by resource_key
            ) as a
            where a.num>=2
        ) as c
        on b.resource_key=c.resource_key
        where c.resource_key is null
        UNION
        select d.data_generate_time,d.user_id,d.kwai_id,d.resource_key
        from(
            select b2.data_generate_time,b2.user_id,b2.kwai_id,b2.resource_key,
                row_number() over (partition by b2.resource_key order by b2.data_generate_time desc) as order_num
            from(
                select data_generate_time,user_id,kwai_id,resource_key
                from default.tmp_kuaishou_user_id_kwai_id_resource_key_data_set
            ) as b2
            left join(
                select a2.resource_key
                from(
                    select resource_key,count(1) as num
                    from default.tmp_kuaishou_user_id_kwai_id_resource_key_data_set
                    group by resource_key
                ) as a2
                where a2.num>=2
            ) as c2
            on b2.resource_key=c2.resource_key
            where c2.resource_key is not null
        ) as d
        where d.order_num=1
    ) as f;
    "
    ${tmp_kuaishou_all_user_id_set}
    echo "#############################################################"

    echo "####################### 快手全量用户信息快照 #################################"
    tmp_kuaishou_all_user_data="
    insert into bigdata.kuaishou_user_data_daily_snapshot partition(dt='${date}')
    select e.data_generate_time,e.app_package_name,e.user_name,e.user_id,e.kwai_id,
        e.user_share_url,e.follower_count,e.following_count,e.signature,
        e.store_or_curriculum,e.curriculum,e.sex,e.constellation,e.certification,e.short_video_count,e.talk_count,e.music_count,e.label3,
        u.user_avatar_url,
        e.app_version,e.resource_key
    from (
        select f2.data_generate_time,f2.app_package_name,f2.user_name,f2.user_id,u2.kwai_id,
            f2.user_share_url,f2.follower_count,f2.following_count,f2.signature,
            f2.store_or_curriculum,f2.curriculum,f2.sex,f2.constellation,f2.certification,f2.short_video_count,f2.talk_count,f2.music_count,f2.label3,
            f2.user_avatar_url,f2.app_version,f2.resource_key

        from(
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
        ) as f2
        join(
            select user_id,kwai_id,resource_key from default.tmp_kuaishou_all_user_id_set
        ) as u2
        on f2.user_id=u2.user_id and f2.resource_key=u2.resource_key
    ) as e
    left join(
        select app_package_name,user_id,user_avatar_url from default.tmp_kuaishou_user_avatar_url_data
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
    ${tmp_kuaishou_user_kwai_id_data}
    ${tmp_kuaishou_user_avatar_url_data}
    ${tmp_kuaishou_all_user_id_kwai_id_resource_key_data}
    ${tmp_kuaishou_user_id_kwai_id_resource_key_data_set}
    ${tmp_kuaishou_all_user_id_set}
    ${tmp_kuaishou_all_user_data}"

