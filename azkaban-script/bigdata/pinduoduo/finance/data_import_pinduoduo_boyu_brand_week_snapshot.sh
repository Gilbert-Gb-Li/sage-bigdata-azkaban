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



    echo "################## 品牌表  ################ "
    tmp_pinduoduo_boyu_brand_week_snapshot="
    INSERT INTO bigdata.pinduoduo_boyu_brand_week_snapshot partition(dt='${date}')
    select
        concat('${stat_date}','PDD_BY_BRAND',e.brand) key_word,
        'pinduoduo' as meta_app_name,
        'pinduoduo_boyu_brand_week_snapshot' as meta_table_name,
        e.brand,
        max(e.week_goods_num) as week_goods_num,
        max(e.week_group_goods_num) as week_group_goods_num,
        max(e.week_shop_num) as week_shop_num,
        max(e.week_goods_total_sale_num) as week_goods_total_sale_num,
        max(e.week_goods_total_money) as week_goods_total_money,
        max(e.week_goods_total_money)/max(e.week_goods_total_sale_num) as week_avg_goods_price,
        max(e.week_goods_total_sale_num)/max(e.week_group_goods_num) week_avg_goods_sale_num
    from (
        select a.brand,count(a.goods_id) as week_goods_num,0 as week_shop_num,0 as week_goods_total_sale_num,0 as week_goods_total_money,0 as week_group_goods_num
        from (
            select distinct brand,goods_id
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_6}' and dt<='${date}'
                and brand is not null and brand!=''
                and goods_id is not null and goods_id!=''
                AND goods_type=1
        ) as a
        group by a.brand
        UNION ALL
        select a1.brand,0 as week_goods_num,0 as week_shop_num,0 as week_goods_total_sale_num,0 as week_goods_total_money,count(a1.goods_id) as week_group_goods_num
        from (
            select distinct brand,goods_id
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_6}' and dt<='${date}'
                and brand is not null and brand!=''
                and goods_id is not null and goods_id!=''
                and goods_group_sale_num>0
                AND goods_type=1
        ) as a1
        group by a1.brand
        UNION ALL
        select b.brand,0 as week_goods_num,count(b.shop_id) as week_shop_num,0 as week_goods_total_sale_num,0 as week_goods_total_money,0 as week_group_goods_num
        from (
            select distinct brand,shop_id
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_6}' and dt<='${date}'
                and brand is not null and brand!=''
                and shop_id is not null and shop_id!=''
                AND goods_type=1
        ) as b
        group by b.brand
        UNION ALL
        select c.brand,0 as week_goods_num,0 as week_shop_num,
            sum(week_goods_group_sale_num) as week_goods_total_sale_num,
            sum(week_goods_money) as week_goods_total_money,0 as week_group_goods_num
        from(
            select distinct brand,goods_id
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_6}' and dt<='${date}'
                and brand is not null and brand!=''
                and goods_id is not null and goods_id!=''
                AND goods_type=1
        ) as c
        join(
            select goods_id,week_goods_group_sale_num,week_goods_money
            from bigdata.pinduoduo_boyu_goods_week_snapshot
            where dt='${date}'
                and week_goods_money>0
        ) as d
        on c.goods_id=d.goods_id
        group by c.brand
    ) as e
    group by e.brand
    ;
    "

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.pinduoduo_boyu_brand_week_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "


    if [ ${week} -eq '1' ]
        then
            echo '################删除HDFS上的数据####################'
            hdfs dfs -rm -r /data/pinduoduo/snapshot/pinduoduo_boyu_brand_week_snapshot/dt=${date}
            echo "${tmp_pinduoduo_boyu_brand_week_snapshot}"
            executeHiveCommand "
            ${delete_hive_partitions}
            ${tmp_pinduoduo_boyu_brand_week_snapshot}
            "

        else
            echo "不是自然周的最后一天，不进行计算！"
    fi



