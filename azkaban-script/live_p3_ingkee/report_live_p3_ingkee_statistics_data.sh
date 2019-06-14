#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

day=$1
yesterday=`date -d "-1 day $day" +%Y-%m-%d`
stat_date=`date -d "$day" +%Y%m%d`
week=`date -d "-6 day $day" +%Y-%m-%d`
year=`date -d "$day" +%Y`
month=`date -d "$day" +%m`


    echo '################获取映客原始数据量 ####################'
    tmp_ingkee_origin_count="
    CREATE TEMPORARY TABLE default.tmp_ingkee_origin_count AS
    select '${day}' as dt,a.appPackageName,
        (max(a.id_list_data_count) +
        max(a.live_danmu_data_count) +
        max(a.gift_contributor_data_count) +
        max(a.gift_info_data_count) +
        max(a.guard_data_count) +
        max(a.stream_url_data_count) +
        max(a.user_info_data_count) +
        max(a.live_viewer_data_count) +
        max(a.weibo_url_data_count)) as origin_data_count,
        max(a.id_list_data_count) as id_list_data_count,
        max(a.live_danmu_data_count) as live_danmu_data_count,
        max(a.gift_contributor_data_count) as gift_contributor_data_count,
        max(a.gift_info_data_count) as gift_info_data_count,
        max(a.guard_data_count) as guard_data_count,
        max(a.stream_url_data_count) as stream_url_data_count,
        max(a.user_info_data_count) as user_info_data_count,
        max(a.live_viewer_data_count) as live_viewer_data_count,
        max(a.weibo_url_data_count) as weibo_url_data_count
    from (
        SELECT '${finance_live_app}' as appPackageName, COUNT(1) AS id_list_data_count, 0 AS live_danmu_data_count, 0 AS gift_contributor_data_count
            , 0 AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_id_list_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, COUNT(1) AS live_danmu_data_count, 0 AS gift_contributor_data_count
            , 0 AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_danmu_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count, COUNT(1) AS gift_contributor_data_count
            , 0 AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_gift_contributor_list_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count, 0 AS gift_contributor_data_count
            , COUNT(1) AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count
            , 0 AS user_info_data_count, 0 AS live_viewer_data_count, 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_gift_info_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , COUNT(1) AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count
            , 0 AS live_viewer_data_count, 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, COUNT(1) AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_stream_url_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, 0 AS stream_url_data_count, COUNT(1) AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_user_info_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, COUNT(1) AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_viewer_list_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , COUNT(1) AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_weibo_url_data_origin_orc
        WHERE dt = '${day}'
            AND appPackageName IN ('${finance_live_app}')
    ) as a
    group by a.appPackageName
    ;
    "
    echo "${tmp_ingkee_origin_count}"

    echo '################获取映客可用原始数据量 ####################'
    tmp_ingkee_valid_origin_count="
    CREATE TEMPORARY TABLE default.tmp_ingkee_valid_origin_count AS
    select '${day}' as dt,a.appPackageName,
        (max(a.id_list_data_count) +
        max(a.live_danmu_data_count) +
        max(a.gift_contributor_data_count) +
        max(a.gift_info_data_count) +
        max(a.guard_data_count) +
        max(a.stream_url_data_count) +
        max(a.user_info_data_count) +
        max(a.live_viewer_data_count) +
        max(a.weibo_url_data_count)) as valid_origin_data_count,
        max(a.id_list_data_count) as valid_id_list_data_count,
        max(a.live_danmu_data_count) as valid_live_danmu_data_count,
        max(a.gift_danmu_data_count) as valid_gift_danmu_data_count,
        max(a.gift_contributor_data_count) as valid_gift_contributor_data_count,
        max(a.gift_info_data_count) as valid_gift_info_data_count,
        max(a.guard_data_count) as valid_guard_data_count,
        max(a.stream_url_data_count) as valid_stream_url_data_count,
        max(a.user_info_data_count) as valid_user_info_data_count,
        max(a.live_viewer_data_count) as valid_live_viewer_data_count,
        max(a.weibo_url_data_count) as valid_weibo_url_data_count
    from (
        SELECT '${finance_live_app}' as appPackageName, COUNT(1) AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count
            , 0 AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_id_list_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and user_name is not null and user_name!=''
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, COUNT(1) AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count
            , 0 AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_danmu_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and content is not null and content!=''
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,count(1) AS gift_danmu_data_count, 0 AS gift_contributor_data_count
            , 0 AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_danmu_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and content is not null and content!=''
            and audience_id is not null and audience_id!=''
            and gift_num is not null and gift_num>0 and gift_num<99999
            and ((gift_id is not null and gift_id!='') or (gift_name is not null and gift_name!='' ))
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, COUNT(1) AS gift_contributor_data_count
            , 0 AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_gift_contributor_list_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and contributor_user_id is not null and contributor_user_id!=''
            and contributor_gift_num is not null and contributor_gift_num>0
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count
            , COUNT(1) AS gift_info_data_count, 0 AS guard_data_count, 0 AS stream_url_data_count
            , 0 AS user_info_data_count, 0 AS live_viewer_data_count, 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_gift_info_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and ((gift_id is not null and gift_id!='') or (gift_name is not null and gift_name!='' ))
            and gift_gold is not null and gift_gold>0
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , COUNT(1) AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count
            , 0 AS live_viewer_data_count, 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_guard_list_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and guarder_id is not null and guarder_id!=''
            and guarder_name is not null and guarder_name!=''
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, COUNT(1) AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_stream_url_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and stream_url is not null and stream_url!=''
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, 0 AS stream_url_data_count, COUNT(1) AS user_info_data_count, 0 AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_user_info_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and user_name is not null and user_name !=''
            and follow_num is not null and follow_num >=0
            and fans_num is not null and fans_num >=0
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, COUNT(1) AS live_viewer_data_count
            , 0 AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_viewer_list_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and audience_id is not null and audience_id !=''
            and audience_name is not null and audience_name !=''
            AND appPackageName IN ('${finance_live_app}')
        UNION ALL
        SELECT '${finance_live_app}' as appPackageName, 0 AS id_list_data_count, 0 AS live_danmu_data_count,0 AS gift_danmu_data_count, 0 AS gift_contributor_data_count, 0 AS gift_info_data_count
            , 0 AS guard_data_count, 0 AS stream_url_data_count, 0 AS user_info_data_count, 0 AS live_viewer_data_count
            , COUNT(1) AS weibo_url_data_count
        FROM ias_p3.tbl_ex_live_weibo_url_data_origin_orc
        WHERE dt = '${day}'
            and data_generate_time is not null and data_generate_time>0
            and user_id is not null and user_id!=''
            and weibo_url is not null and weibo_url !=''
            AND appPackageName IN ('${finance_live_app}')
    ) as a
    group by a.appPackageName
    ;
    "

    echo "${tmp_ingkee_valid_origin_count}"


    echo '################主播统计信息存储hive####################'
    ingkee_statistics_data_save_hive="
    insert into table live_p3_finance.tbl_ex_ingkee_statistics_data partition(dt='${day}')
    SELECT  concat('${stat_date}','_ingkee_origin_statistics') as keyWord,
            'ingkee' as meta_app_name,
            'tbl_ex_ingkee_statistics_data' as meta_table_name,
            COALESCE(a.appPackageName,b.appPackageName) as appPackageName,
            a.origin_data_count,
            b.valid_origin_data_count,
            a.id_list_data_count,
            b.valid_id_list_data_count,
            a.live_danmu_data_count,
            b.valid_live_danmu_data_count,
            b.valid_gift_danmu_data_count,
            a.gift_contributor_data_count,
            b.valid_gift_contributor_data_count ,
            a.gift_info_data_count ,
            b.valid_gift_info_data_count ,
            a.guard_data_count ,
            b.valid_guard_data_count ,
            a.stream_url_data_count ,
            b.valid_stream_url_data_count ,
            a.user_info_data_count ,
            b.valid_user_info_data_count ,
            a.live_viewer_data_count ,
            b.valid_live_viewer_data_count ,
            a.weibo_url_data_count ,
            b.valid_weibo_url_data_count
    FROM(
        SELECT dt, apppackagename, origin_data_count, id_list_data_count, live_danmu_data_count
            , gift_contributor_data_count, gift_info_data_count, guard_data_count, stream_url_data_count, user_info_data_count
            , live_viewer_data_count, weibo_url_data_count
        FROM DEFAULT.tmp_ingkee_origin_count
    ) as a
    JOIN(
        SELECT dt, apppackagename, valid_origin_data_count, valid_id_list_data_count, valid_live_danmu_data_count
            , valid_gift_danmu_data_count, valid_gift_contributor_data_count, valid_gift_info_data_count, valid_guard_data_count, valid_stream_url_data_count
            , valid_user_info_data_count, valid_live_viewer_data_count, valid_weibo_url_data_count
        FROM DEFAULT.tmp_ingkee_valid_origin_count
    ) as b
    on a.dt=b.dt and a.appPackageName=b.appPackageName
    ;
    "
    echo "${ingkee_statistics_data_save_hive}"


    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE live_p3_finance.tbl_ex_ingkee_statistics_data DROP IF EXISTS PARTITION (dt='${day}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rmr /data/live_p3_finance/tbl_ex_ingkee_statistics_data/dt=${day}


    executeHiveCommand "
    ${delete_hive_partitions}
    ${tmp_ingkee_origin_count}
    ${tmp_ingkee_valid_origin_count}
    ${ingkee_statistics_data_save_hive}
    "

