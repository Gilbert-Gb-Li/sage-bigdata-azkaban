#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="
    INSERT INTO bigdata.pinduoduo_boyu_shop_new_snapshot partition(dt='${date}')
    select t1.data_generate_time, t1.url, t1.brand
        , t1.shop_id, t1.shop_name, t1.shop_goods_num, t1.shop_goods_all_sale_num,t1.shop_type_img
    from(
        select a.data_generate_time, a.url, a.brand
            , a.shop_id, a.shop_name, a.shop_goods_num, a.shop_goods_all_sale_num,a.shop_type_img
            , b.shop_id as b_shop_id
        from (
            SELECT data_generate_time, url, brand
                , shop_id, shop_name, shop_goods_num, shop_goods_all_sale_num,shop_type_img
            FROM bigdata.pinduoduo_boyu_shop_daily_snapshot
            WHERE (dt = '${date}'
                AND shop_id IS NOT NULL
                AND shop_id != '')
        ) as a
        left join(
            SELECT data_generate_time, url, brand
                , shop_id, shop_name, shop_goods_num, shop_goods_all_sale_num,shop_type_img
            FROM bigdata.pinduoduo_boyu_shop_daily_snapshot
            WHERE (dt = '${yesterday}'
                AND shop_id IS NOT NULL
                AND shop_id != '')
        ) as b
        on a.shop_id=b.shop_id
    ) as t1
    where t1.b_shop_id is  null;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.pinduoduo_boyu_shop_new_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/pinduoduo/snapshot/pinduoduo_boyu_shop_new_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql}"

