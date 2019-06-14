#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "$date" +%Y%m`
date_add_1=`date -d "+1 day $date" +%Y-%m-%d`

yesterday=`date -d "-1 day $date" +%Y-%m-%d`
date_reduce_1_1=`date -d "-1 day $date" +%Y%m%d`

date_reduce_6=`date -d "-6 day $date" +%Y-%m-%d`

date_reduce_7=`date -d "-7 day $date" +%Y-%m-%d`
date_reduce_7_1=`date -d "-7 day $date" +%Y%m%d`

date_reduce_13=`date -d "-13 day $date" +%Y-%m-%d`
date_reduce_13_1=`date -d "-13 day $date" +%Y%m%d`

week=`date -d "${date_add_1}" +%w`
echo "周：${week}"
month=`date -d "${date}" +%Y%m`
echo "月格式1：${month}"
month1=`date -d "${date}" +%Y-%m`
echo "月格式2：${month1}"
month1_01=`date -d "${month1}-01" +%Y-%m-%d`
month1_01_1=`date -d "${month1}-01" +%Y%m%d`
echo "月第一天：${month1_01}"
month1_01_reduce_1=`date -d "-1 day $month1_01" +%Y-%m-%d`
month1_01_reduce_1_1=`date -d "-1 day $month1_01" +%Y%m%d`
echo "上月最后一天：${month1_01_reduce_1}"
month2=`date -d "${month1_01_reduce_1}" +%Y-%m`
echo "上月格式2：${month2}"
month2_01=`date -d "${month2}-01" +%Y-%m-%d`
month2_01_1=`date -d "${month2}-01" +%Y%m%d`
echo "上月第一天：${month2_01}"
day=`date -d "${date_add_1}" +%d`



    echo "################## 平台表  ################ "
    tmp_pinduoduo_boyu_platform_week_snapshot="
    INSERT INTO bigdata.pinduoduo_boyu_platform_week_snapshot partition(dt='${date}')
    select concat('${stat_date}','PDD_BY_PLATFORM') key_word,
        a.meta_app_name,
        'pinduoduo_boyu_platform_week_snapshot' as meta_table_name,
        max(a.total_shop_count) as total_shop_count,
        max(a.week_shop_count) as week_shop_count,
        max(a.week_new_shop_count) as week_new_shop_count,
        max(a.total_goods_count) as total_goods_count,
        max(a.week_goods_count) as week_goods_count,
        max(a.week_group_goods_count) as week_group_goods_count,
        max(a.week_new_goods_count) as week_new_goods_count,
        max(a.week_brand_count) as week_brand_count,
        max(a.week_goods_total_money) as week_goods_total_money,
        max(a.week_goods_flagship_count) as week_goods_flagship_count,
        max(a.week_goods_brand_count) as week_goods_brand_count,
        max(a.week_shop_flagship_count) as week_shop_flagship_count
    from(
        select 'pinduoduo' as meta_app_name,count(1) as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_shop_daily_snapshot
        where dt='${date}'
            and shop_id is not null and shop_id!=''
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,count(distinct shop_id) as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_goods_origin
        where dt>='${date_reduce_6}' and dt<='${date}'
            and shop_id is not null and shop_id!='' AND goods_type=1
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,0 as week_shop_count,count(1) as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_shop_new_snapshot
        where dt>='${date_reduce_6}' and dt<='${date}'
            and shop_id is not null and shop_id!=''
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            count(1) as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_goods_daily_snapshot
        where dt='${date}'
            and goods_id is not null and goods_id!=''
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,count(distinct goods_id) as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_goods_origin
        where dt>='${date_reduce_6}' and dt<='${date}'
            and goods_id is not null and goods_id!='' AND goods_type=1
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,count(1) as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_goods_new_snapshot
        where dt>='${date_reduce_6}' and dt<='${date}'
            and goods_id is not null and goods_id!=''
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,count(distinct brand) as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_goods_origin
        where dt>='${date_reduce_6}' and dt<='${date}'
            and brand is not null and brand!='' AND goods_type=1
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,sum(week_goods_money) as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_goods_week_snapshot
        where dt='${date}'
            and goods_id is not null and goods_id!=''
            and week_goods_money>0
        UNION ALL
        select 'pinduoduo' as meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,count(distinct goods_id) as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,0 as week_shop_flagship_count
        from bigdata.pinduoduo_boyu_goods_origin
        where dt>='${date_reduce_6}' and dt<='${date}'
            and goods_id is not null and goods_id!=''
            and goods_group_sale_num>0
            AND goods_type=1
        UNION ALL
        select a1.meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            sum(a1.goods_flagship) as week_goods_flagship_count,
            sum(a1.goods_brand) as week_goods_brand_count,0 as week_shop_flagship_count
        from(
            select distinct 'pinduoduo' as meta_app_name,goods_id,if(instr(goods_type_img,'e01ec845d67c3717fd0581da8dbf86bd')>0,1,0) as goods_flagship,
                if(instr(goods_type_img,'1db06b5aec36d7409e056e3fa51badc5')>0,1,0) as goods_brand
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_6}' and dt<='${date}'
                and goods_id is not null and goods_id!=''
                AND goods_type=1
                AND goods_type_img is not null and goods_type_img!=''
        ) as a1
        group by a1.meta_app_name
        UNION ALL
        select a2.meta_app_name,0 as total_shop_count,0 as week_shop_count,0 as week_new_shop_count,
            0 as total_goods_count,0 as week_goods_count,0 as week_new_goods_count,0 as week_brand_count,0 as week_goods_total_money,0 as week_group_goods_count,
            0 as week_goods_flagship_count,0 as week_goods_brand_count,sum(a2.shop_flagship) as week_shop_flagship_count
        from (
            select distinct 'pinduoduo' as meta_app_name,shop_id,if(instr(shop_type_img,'90e6990661ea5c56ac6613bfabc76795')>0,1,0) as shop_flagship
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_6}' and dt<='${date}'
                and shop_id is not null and shop_id!=''
                AND goods_type=1
                AND shop_type_img is not null and shop_type_img!=''
        ) as a2
        group by a2.meta_app_name
    ) as a
    group by a.meta_app_name
    ;
    "


    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.pinduoduo_boyu_platform_week_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "



    if [ ${week} -eq '1' ]
        then
            echo '################删除HDFS上的数据####################'
            hdfs dfs -rm -r /data/pinduoduo/snapshot/pinduoduo_boyu_platform_week_snapshot/dt=${date}

            echo "${tmp_pinduoduo_boyu_platform_week_snapshot}"
            executeHiveCommand "
            ${delete_hive_partitions}
            ${tmp_pinduoduo_boyu_platform_week_snapshot}
            "

        else
            echo "不是自然周的最后一天，不进行计算！"
    fi



