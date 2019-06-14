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
    tmp_pinduoduo_boyu_platform_week_to_es="
    INSERT INTO bigdata.pinduoduo_boyu_platform_week_es_data
    select '${stat_month}' as stat_month,dt,key_word,meta_app_name,meta_table_name,total_shop_count,week_shop_count,week_new_shop_count,
        total_goods_count,week_goods_count,week_group_goods_count,week_new_goods_count,week_brand_count,week_goods_total_money,
        week_goods_flagship_count,week_goods_brand_count,week_shop_flagship_count
    from bigdata.pinduoduo_boyu_platform_week_snapshot
    where dt='${date}';
    "

    tmp_pinduoduo_boyu_brand_week_to_es="
    INSERT INTO bigdata.pinduoduo_boyu_brand_week_es_data
    select '${stat_month}' as stat_month,dt,key_word,meta_app_name,meta_table_name,brand,week_goods_num,week_group_goods_num,week_shop_num,
        week_goods_total_sale_num,week_goods_total_money,week_avg_goods_price,week_avg_goods_sale_num
    from bigdata.pinduoduo_boyu_brand_week_snapshot
    where dt='${date}';
    "


    tmp_pinduoduo_boyu_goods_week_to_es="
    INSERT INTO bigdata.pinduoduo_boyu_goods_week_es_data
    select '${stat_month}' as stat_month,dt,key_word,meta_app_name,meta_table_name,goods_id,goods_name,goods_group_sale_num,
        week_goods_group_sale_num,goods_price,week_goods_money,goods_comment_num,week_goods_comment_num,goods_total_station_sale_num
    from bigdata.pinduoduo_boyu_goods_week_snapshot
    where dt='${date}';
    "

    if [ ${week} -eq '1' ]
        then
            echo "${tmp_pinduoduo_boyu_platform_week_to_es}"
            echo "${tmp_pinduoduo_boyu_brand_week_to_es}"
            echo "${tmp_pinduoduo_boyu_goods_week_to_es}"
            executeHiveCommand "
            add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
            add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
            ${tmp_pinduoduo_boyu_platform_week_to_es}
            ${tmp_pinduoduo_boyu_brand_week_to_es}
            ${tmp_pinduoduo_boyu_goods_week_to_es}
            "
        else
            echo "不是自然周的最后一天，不进行计算！"
    fi



