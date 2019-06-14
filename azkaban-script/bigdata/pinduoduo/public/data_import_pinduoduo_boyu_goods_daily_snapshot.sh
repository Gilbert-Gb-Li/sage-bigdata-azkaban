#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="
INSERT INTO bigdata.pinduoduo_boyu_goods_daily_snapshot partition(dt='${date}')
select t1.data_generate_time, t1.url, t1.brand, t1.goods_id, t1.goods_name
    , t1.shop_id, t1.shop_name, t1.shop_goods_num, t1.shop_goods_all_sale_num, t1.goods_group_sale_num
    , t1.goods_total_station_sale_num, t1.goods_price, t1.goods_comment_num
    , t1.goods_is_on_sale,t1.goods_type_img,t1.goods_desc
from  (
        select a.data_generate_time, a.url, a.brand, a.goods_id, a.goods_name
            , a.shop_id, a.shop_name, a.shop_goods_num, a.shop_goods_all_sale_num, a.goods_group_sale_num
            , a.goods_total_station_sale_num, a.goods_price, a.goods_comment_num
            , a.goods_is_on_sale,a.goods_type_img,a.goods_desc
            , row_number() over (partition by a.goods_id order by a.data_generate_time desc) as row_num
        from(
            SELECT data_generate_time, url, brand, goods_id, goods_name
                , shop_id, shop_name, shop_goods_num, shop_goods_all_sale_num, goods_group_sale_num
                , goods_total_station_sale_num, goods_price, goods_comment_num
                , goods_is_on_sale,goods_type_img,goods_desc
            FROM bigdata.pinduoduo_boyu_goods_origin
            WHERE (dt = '${date}'
                AND goods_type=1
                AND goods_id IS NOT NULL
                AND goods_id != '')
            UNION
            SELECT data_generate_time, url, brand, goods_id, goods_name
                , shop_id, shop_name, shop_goods_num, shop_goods_all_sale_num, goods_group_sale_num
                , goods_total_station_sale_num, goods_price, goods_comment_num
                , goods_is_on_sale,goods_type_img,goods_desc
            FROM bigdata.pinduoduo_boyu_goods_daily_snapshot
            WHERE (dt = '${yesterday}'
                AND goods_id IS NOT NULL
                AND goods_id != '')
        ) as a
      ) t1
where t1.row_num =1;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.pinduoduo_boyu_goods_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/pinduoduo/snapshot/pinduoduo_boyu_goods_daily_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql}
"

