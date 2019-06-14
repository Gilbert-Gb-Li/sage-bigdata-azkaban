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


    echo "################### 获取送礼用户信息 #############################"
    tmp_finance_kuaishou_like_audience_all_data="
    CREATE TEMPORARY TABLE default.tmp_finance_kuaishou_like_audience_all_data AS
    select a.app_package_name,
        a.audience_id as user_id,
        if(c.kwai_id is not null,c.kwai_id,'') as kwai_id,
        a.user_id as receive_gift_user_id,
        if(b.kwai_id is not null,b.kwai_id,'') as receive_gift_kwai_id,
        a.gift_val as receive_gift_val
    from(
        select app_package_name,user_id,audience_id,sum(gift_val) as gift_val
        from bigdata.kuaishou_live_danmu_gift_data
        where dt='${date}' and app_package_name='${apk_name}'
            and user_id is not null and user_id!=''
            and audience_id is not null and audience_id!=''
            and gift_val>0
        group by app_package_name,user_id,audience_id
    ) as a
    left join(
        select app_package_name,user_id,kwai_id
        from bigdata.kuaishou_user_data_daily_snapshot
        where dt='${date}' and app_package_name='${apk_name}'
            and user_id is not null and user_id!=''
            and kwai_id is not null and kwai_id!=''
    ) as b
    on a.app_package_name=b.app_package_name and a.user_id=b.user_id
    left join(
        select app_package_name,user_id,kwai_id
        from bigdata.kuaishou_user_data_daily_snapshot
        where dt='${date}' and app_package_name='${apk_name}'
            and user_id is not null and user_id!=''
            and kwai_id is not null and kwai_id!=''
    ) as c
    on a.app_package_name=c.app_package_name and a.audience_id=c.user_id;
    "
    echo "${tmp_finance_kuaishou_like_audience_all_data}"
    echo "##########################################################"

    echo "################### 付费用户信息存HDSF #############################"
    tmp_finance_kuaishou_like_audience_all_data_to_hvie="
    insert into bigdata.kuaishou_live_audience_all_data partition(dt='${date}')
    select 'kauishou' as  meta_app_name, 'kuaishou_live_audience_all_data' as meta_table_name,
        app_package_name,user_id,kwai_id,receive_gift_user_id,receive_gift_kwai_id,receive_gift_val
    from default.tmp_finance_kuaishou_like_audience_all_data;
    "
    echo "${tmp_finance_kuaishou_like_audience_all_data_to_hvie}"
    echo "##########################################################"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.kuaishou_live_audience_all_data DROP IF EXISTS PARTITION (dt='${date}');
    "
    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/kuaishou/snapshot/kuaishou_live_audience_all_data/dt=${date}

    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_finance_kuaishou_like_audience_all_data}
    ${tmp_finance_kuaishou_like_audience_all_data_to_hvie}
    "
