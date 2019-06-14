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
echo "year:${year}"
echo "month:${month}"

apk_name="com.smile.gifmaker"


    echo "################### 获取礼物金额和打赏人数 #############################"
    tmp_finance_kuaishou_like_user_all_gift_val_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_like_user_all_gift_val_data AS
    select a.app_package_name,a.user_id,count(1) as user_cost_gift_count,sum(gift_val) as gift_val
    from(
        select app_package_name,user_id,audience_id,sum(gift_val) as gift_val
        from bigdata.kuaishou_live_danmu_gift_data
        where dt='${date}' and app_package_name='${apk_name}'
            and user_id is not null and user_id!=''
            and audience_id is not null and audience_id!=''
            and gift_val>0
        group by app_package_name,user_id,audience_id
    ) as a
    group by a.app_package_name,a.user_id;
    "
    echo "${tmp_finance_kuaishou_like_user_all_gift_val_data}"
    echo "##########################################################"

    echo "################### 获取观众数 #############################"
    tmp_finance_kuaishou_like_user_audience_count_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_like_user_audience_count_data AS
    select app_package_name,user_id,max(audience_count) as audience_count
    from bigdata.kuaishou_live_end_data_origin_orc
    where dt='${date}' and app_package_name='${apk_name}'
        and user_id is not null and user_id!=''
        and audience_count>0
    group by app_package_name,user_id;
    "
    echo "${tmp_finance_kuaishou_like_user_audience_count_data}"
    echo "##########################################################"

    echo "################### 获取直播主播id #############################"
    tmp_finance_kuaishou_like_user_id_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_like_user_id_data AS
    select  a.app_package_name,a.user_id,
        if(b.user_name is not null,b.user_name,'') as user_name
    from(
        select distinct app_package_name,user_id from bigdata.kuaishou_live_end_data_origin_orc where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!=''
        union
        select distinct app_package_name,user_id from bigdata.kuaishou_live_danmu_data_origin_orc where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!=''
        union
        select distinct app_package_name,user_id from bigdata.kuaishou_live_user_info_data_origin_orc where dt='${date}' and app_package_name='${apk_name}' and user_id is not null and user_id!=''
    ) as a
    left join(
        select  t.app_package_name,t.user_id,t.user_name
        from(
            select *,row_number() over (partition by app_package_name,user_id order by data_generate_time desc) as order_num
            from bigdata.kuaishou_live_user_info_data_origin_orc
            where dt='${date}' and app_package_name='${apk_name}'
            and user_id is not null and user_id!=''
        ) as t
        where t.order_num=1
    ) as b
    on a.app_package_name=b.app_package_name and a.user_id =b.user_id;
    "
    echo "${tmp_finance_kuaishou_like_user_id_data}"
    echo "##########################################################"


    echo "################### 整合直播主播信息 #############################"
    tmp_finance_kuaishou_like_user_all_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_like_user_all_data AS
    select c1.app_package_name,c1.user_name,c1.user_id,c1.kwai_id,c1.user_share_url,c1.signature,c1.store_or_curriculum,c1.hometown,c1.sex,c1.constellation,c1.certification,
        if(c1.follower_count is not null and c1.follower_count>0,c1.follower_count,0) as follower_count,
        if(c1.following_count is not null and c1.following_count>0,c1.following_count,0) as following_count,
        if(c1.short_video_count is not null and c1.short_video_count>0,c1.short_video_count,0) as short_video_count,
        if(c1.talk_count is not null and c1.talk_count>0,c1.talk_count,0) as talk_count,
        if(c1.music_count is not null and c1.music_count>0,c1.music_count,0) as music_count,
        if(d1.gift_val is not null and d1.gift_val>0,d1.gift_val,0) as gift_val,
        if(d1.user_cost_gift_count is not null and d1.user_cost_gift_count>0,d1.user_cost_gift_count,0) as user_cost_gift_count,
        if(e1.audience_count is not null and e1.audience_count>0,e1.audience_count,0) as online_count_max,
        0 as online_count_sum
    from(
        select b1.app_package_name,if(b1.user_name!='',b1.user_name,a1.user_name) as user_name,
            b1.user_id,a1.kwai_id,a1.user_share_url,a1.signature,a1.store_or_curriculum,a1.hometown,a1.sex,a1.constellation,a1.certification,
            a1.follower_count,a1.following_count,a1.short_video_count,a1.talk_count,a1.music_count
        from(
            select app_package_name,user_name,user_id,if(kwai_id is null,'',kwai_id) as kwai_id,user_share_url,signature,
                if(store_or_curriculum>0,store_or_curriculum,curriculum) as store_or_curriculum,
                if(label3 is null,'',label3) as hometown,sex,constellation,certification,
                follower_count,following_count,short_video_count,talk_count,music_count
            from bigdata.kuaishou_user_data_daily_snapshot
            where dt='${date}' and app_package_name='${apk_name}'
                and user_id is not null and user_id!=''
        ) as a1
        right join(
            select app_package_name,user_id,user_name from default.tmp_finance_kuaishou_like_user_id_data
        ) as b1
        on a1.app_package_name=b1.app_package_name and a1.user_id=b1.user_id
    ) as c1
    left join(
        select app_package_name,user_id,user_cost_gift_count,gift_val from default.tmp_finance_kuaishou_like_user_all_gift_val_data
    ) as d1
    on c1.app_package_name=d1.app_package_name and c1.user_id=d1.user_id
    left join(
        select app_package_name,user_id,audience_count from default.tmp_finance_kuaishou_like_user_audience_count_data
    ) as e1
    on c1.app_package_name=e1.app_package_name and c1.user_id=e1.user_id;
    "
    echo "${tmp_finance_kuaishou_like_user_all_data}"
    echo "##########################################################"


    echo "################### 直播用户信息存HDFS #############################"
    tmp_finance_kuaishou_like_user_all_data_to_hdfs="
    insert into bigdata.kuaishou_live_user_all_data partition(dt='${date}')
    select 'kauishou' as  meta_app_name, 'kuaishou_live_user_all_data' as meta_table_name,
        app_package_name,user_name,user_id,kwai_id,user_share_url,signature,store_or_curriculum,hometown,sex,constellation,certification,
        follower_count,following_count,short_video_count,talk_count,music_count,
        gift_val,user_cost_gift_count,online_count_max,online_count_sum
    from default.tmp_finance_kuaishou_like_user_all_data;
    "
    echo "${tmp_finance_kuaishou_like_user_all_data_to_hdfs}"
    echo "##########################################################"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_live_user_all_data DROP IF EXISTS PARTITION (dt='${date}');
    "
    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_live_user_all_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_finance_kuaishou_like_user_all_gift_val_data}
    ${tmp_finance_kuaishou_like_user_audience_count_data}
    ${tmp_finance_kuaishou_like_user_id_data}
    ${tmp_finance_kuaishou_like_user_all_data}
    ${tmp_finance_kuaishou_like_user_all_data_to_hdfs}
    "
