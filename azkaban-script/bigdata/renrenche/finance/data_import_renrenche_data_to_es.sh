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



    echo "################## 平台统计  ################ "
    renrenche_platform_es_data="
    INSERT INTO bigdata.renrenche_platform_es_data
    select '${stat_month}' as stat_month,
        dt,
        concat('${stat_date}','RRC_BY_PLATFORM') key_word,
        'renrenche' as meta_app_name,
        'renrenche_platform_snapshot' as meta_table_name,
        apppackagename,
        on_sale_car_count,
        on_sale_car_count_origin,
        sale_out_car_count,
        new_sale_out_car_count,
        sold_out_car_count,
        new_sold_out_car_count,
        new_car_count,
        sale_out_car_all_price,
        sale_out_car_all_service_charge,
        sale_out_car_avg_price,
        sale_out_car_avg_period,
        on_sale_car_avg_period,
        car_all_count
    from bigdata.renrenche_platform_snapshot
    where dt='${date}';
    "

    echo "##################  车辆信息  ################ "
    renrenche_car_info_finance_es_data="
    INSERT INTO bigdata.renrenche_car_info_finance_es_data
    select '${stat_month}' as stat_month,
        dt,
        concat('${stat_date}',car_state,'car_info',car_uid) key_word,
        'renrenche' as meta_app_name,
        'renrenche_car_info_finance_snapshot' as meta_table_name,
        data_generate_time,
        first_data_generate_time,
        apppackagename,
        car_uid,
        car_source_id,
        car_name,
        car_brand,
        car_series,
        car_type,
        emission_standard,
        car_price,
        service_charge,
        period,
        price_range,
        car_state
    from bigdata.renrenche_car_info_finance_snapshot
    where dt='${date}';
    "

    executeHiveCommand "
    add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
    add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
    ${renrenche_platform_es_data}
    ${renrenche_car_info_finance_es_data}
    "


