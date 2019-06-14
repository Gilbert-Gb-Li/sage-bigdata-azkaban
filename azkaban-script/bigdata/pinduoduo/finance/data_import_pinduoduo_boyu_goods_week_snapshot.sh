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



    echo "################## 商品表  ################ "
    tmp_pinduoduo_boyu_goods_week_snapshot="
    INSERT INTO bigdata.pinduoduo_boyu_goods_week_snapshot partition(dt='${date}')
    select
        concat('${stat_date}','PDD_BY_GOODS',a1.goods_id) key_word,
        'pinduoduo' as meta_app_name,
        'pinduoduo_boyu_goods_week_snapshot' as meta_table_name,
        a1.goods_id,
        a1.goods_name,
        if(a1.goods_group_sale_num>0,a1.goods_group_sale_num,0) as goods_group_sale_num,
        if(b1.goods_group_sale_num>0 and a1.goods_group_sale_num>b1.goods_group_sale_num,a1.goods_group_sale_num-b1.goods_group_sale_num,
            if(b1.goods_group_sale_num>0 and a1.goods_group_sale_num=b1.goods_group_sale_num,0,
                if(a1.goods_group_sale_num>0,a1.goods_group_sale_num,0))) as week_goods_group_sale_num,
        if(a1.goods_price>0,a1.goods_price,0) as goods_price,
        if(b1.goods_group_sale_num>0 and a1.goods_group_sale_num>b1.goods_group_sale_num,a1.goods_group_sale_num-b1.goods_group_sale_num,
            if(b1.goods_group_sale_num>0 and a1.goods_group_sale_num=b1.goods_group_sale_num,0,
                if(a1.goods_group_sale_num>0,a1.goods_group_sale_num,0)))*if(a1.goods_price>0,a1.goods_price,0) as week_goods_money,
        if(a1.goods_comment_num>0,a1.goods_comment_num,0) as goods_comment_num,
        if(b1.goods_comment_num>0 and a1.goods_comment_num>b1.goods_comment_num,a1.goods_comment_num-b1.goods_comment_num,
            if(b1.goods_comment_num>0 and a1.goods_comment_num=b1.goods_comment_num,0,
                if(a1.goods_comment_num>0,a1.goods_comment_num,0))) as week_goods_comment_num,
        if(a1.goods_total_station_sale_num>0,a1.goods_total_station_sale_num,0) as goods_total_station_sale_num

    from(
        select a.goods_id,a.goods_name,a.goods_group_sale_num,a.goods_total_station_sale_num
                , a.goods_price, a.goods_comment_num
        from (
            select  data_generate_time,goods_id, goods_name, goods_group_sale_num,goods_total_station_sale_num
                , goods_price, goods_comment_num
                , row_number() over (partition by goods_id order by data_generate_time desc) as row_num
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_6}' and dt<='${date}'
                and goods_id is not null and goods_id!=''
                AND goods_type=1
                and goods_is_on_sale=1
        ) as a
        where a.row_num=1
    ) as a1
    left join(
        select b.goods_id,b.goods_name,b.goods_group_sale_num,b.goods_total_station_sale_num
                , b.goods_price, b.goods_comment_num
        from (
            select data_generate_time,goods_id, goods_name, goods_group_sale_num,goods_total_station_sale_num
                , goods_price, goods_comment_num
                , row_number() over (partition by goods_id order by data_generate_time desc) as row_num
            from bigdata.pinduoduo_boyu_goods_origin
            where dt>='${date_reduce_13}' and dt<='${date_reduce_7}'
                and goods_id is not null and goods_id!=''
                AND goods_type=1
                and goods_is_on_sale=1
        ) as b
        where b.row_num=1
    ) as b1
    on a1.goods_id=b1.goods_id
    ;
    "

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.pinduoduo_boyu_goods_week_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "



    if [ ${week} -eq '1' ]
        then

            echo '################删除HDFS上的数据####################'
            hdfs dfs -rm -r /data/pinduoduo/snapshot/pinduoduo_boyu_goods_week_snapshot/dt=${date}
            echo "${tmp_pinduoduo_boyu_goods_week_snapshot}"
            executeHiveCommand "
            ${delete_hive_partitions}
            ${tmp_pinduoduo_boyu_goods_week_snapshot}
            "

        else
            echo "不是自然周的最后一天，不进行计算！"
    fi



