#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p3_util.sh

day=$1


#echo "################# 复制mysql数据到hive start ########################"
#
#tmp_gift_info_file=${tmpDir}/tmp_tbl_live_p3_gift_info.txt
#rm -rf ${tmp_gift_info_file}
#
#
#${mysql} -h${mysql_host} -P3306 -u${mysql_user} -p${mysql_password} --default-character-set=utf8 -e "SELECT a.gift_key, AVG(a.gift_unit_val) AS gift_unit_val, MAX(a.data_generate_time) AS data_generate_time FROM ( SELECT concat(a.appPackageName, '-', a.gift_id) AS gift_key, a.gift_unit_val , a.data_generate_time FROM sage_bigdata.tbl_live_p3_all_gift_info a, ( SELECT appPackageName, gift_id, MAX(data_generate_time) AS data_generate_time FROM sage_bigdata.tbl_live_p3_all_gift_info GROUP BY appPackageName, gift_id ) b WHERE (a.appPackageName = b.appPackageName AND a.gift_id = b.gift_id AND a.data_generate_time = b.data_generate_time) UNION ALL SELECT concat(a.appPackageName, '-', a.gift_name) AS gift_key, a.gift_unit_val , a.data_generate_time FROM sage_bigdata.tbl_live_p3_all_gift_info a, ( SELECT appPackageName, gift_name, MAX(data_generate_time) AS data_generate_time FROM sage_bigdata.tbl_live_p3_all_gift_info GROUP BY appPackageName, gift_name ) b WHERE (a.appPackageName = b.appPackageName AND a.gift_name = b.gift_name AND a.data_generate_time = b.data_generate_time) ) a GROUP BY a.gift_key" > ${tmp_gift_info_file}
#
#
#sed -i '1d' ${tmp_gift_info_file}
#
#gift_info_table="tbl_live_p3_gift_info"
#drop_table_gift_info_table="drop table live_p3.${gift_info_table};"
#echo "${drop_table_gift_info_table}"
#
#create_gift_info_table="
#CREATE TABLE IF NOT EXISTS live_p3.${gift_info_table}(
#  gift_key STRING,
#  gift_unit_val FLOAT,
#  data_generate BIGINT
#  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
#"
#echo "${create_gift_info_table}"
#
#
# 因为有HDFS全量的礼物信息表，所以不需要mysql礼物表，并且每天拉取mysql消耗性能
# executeHiveCommand "${drop_table_gift_info_table} ${create_gift_info_table} LOAD DATA LOCAL INPATH '${tmp_gift_info_file}' INTO TABLE live_p3.${gift_info_table};"
#
#echo "############### 复制mysql数据到hive end #####################"

echo "############### 获取HDFS礼物信息 start #####################"

    tmp_hdfs_ingkee_gift_info="CREATE TEMPORARY TABLE default.tmp_hdfs_ingkee_gift_info AS
    SELECT c.gift_key as gift_key,AVG(c.gift_gold) AS gift_gold, AVG(c.gift_unit_val) AS gift_unit_val, MAX(c.data_generate_time) AS data_generate_time
    FROM (
        SELECT concat(a.appPackageName, '-', a.gift_id) AS gift_key,a.gift_gold, a.gift_unit_val
            , a.data_generate_time
        FROM (
            select *,row_number() over (partition by appPackageName,gift_id order by data_generate_time desc) as order_num
            from live_p3.tbl_ex_live_gift_data_daily_snapshot
            where dt='${day}'
        ) as a
        where a.order_num=1
        UNION ALL
        SELECT concat(b.appPackageName, '-', b.gift_name) AS gift_key,b.gift_gold,b.gift_unit_val
            , b.data_generate_time
        FROM (
            select *,row_number() over (partition by appPackageName,gift_name order by data_generate_time desc) as order_num
            from live_p3.tbl_ex_live_gift_data_daily_snapshot
            where dt='${day}'
        ) as b
        where b.order_num=1
    ) as c
    GROUP BY c.gift_key;
    "

echo "${tmp_hdfs_ingkee_gift_info}"
echo "############### 获取HDFS礼物信息 end #####################"


echo "############### 礼物弹幕快照表 start #####################"
    tmp_ingkee_tbl_ex_live_p3_gift_danmu_data="CREATE TEMPORARY TABLE default.tmp_ingkee_tbl_ex_live_p3_gift_danmu_data AS
    SELECT dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id
        , audience_id, audience_name, gift_id, gift_type, gift_name
        , gift_image_url, gift_num, content, gift_unit_price, type
        , gift_type_id, gift_unit_val, gift_val, hour
    FROM ias_p3.tbl_ex_live_danmu_data_origin_orc
    WHERE (dt = '${day}'
        AND ((gift_id IS NOT NULL
                AND gift_id != '')
            OR (gift_name IS NOT NULL
                AND gift_name != ''))
        AND gift_num > 0
        AND gift_val > 0)
    UNION ALL
    SELECT a.dataSource, a.record_time, a.trace_id, a.schema, a.client_time
        , a.cloudServiceId, a.spiderVersion, a.appVersion, a.containerId, a.resourceKey
        , a.dataType, a.data_generate_time, a.appPackageName, a.room_id,
        if(substr(a.user_id,1,1)='@',substr(a.user_id,4),if(substr(a.user_id,1,1)=' ',substr(a.user_id,2),a.user_id )) as user_id
        , a.audience_id, a.audience_name, a.gift_id, a.gift_type, a.gift_name
        , a.gift_image_url, a.gift_num, a.content, a.gift_unit_price, a.type
        , a.gift_type_id
        , if(d.gift_unit_val IS NOT NULL,d.gift_unit_val,e.gift_unit_val) AS gift_unit_val
        , if(d.gift_unit_val IS NOT NULL,d.gift_unit_val * a.gift_num,
            if(e.gift_unit_val IS NOT NULL,e.gift_unit_val * a.gift_num,-1)) AS gift_val
        , a.hour
    FROM (
        SELECT dataSource, record_time, trace_id, schema, client_time
            , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
            , dataType, data_generate_time, appPackageName, room_id, user_id
            , audience_id, audience_name, gift_id, gift_type, gift_name
            , gift_image_url, gift_num, content, gift_unit_price, type
            , gift_type_id, gift_unit_val, gift_val, hour
            , concat(appPackageName, '-', gift_id) AS app_id
            , concat(appPackageName, '-', gift_name) AS app_name
        FROM ias_p3.tbl_ex_live_danmu_data_origin_orc
        WHERE (dt = '${day}'
            AND ((gift_id IS NOT NULL
                    AND gift_id != '')
                OR (gift_name IS NOT NULL
                    AND gift_name != ''))
            AND gift_num > 0
            AND gift_val < 0)
    ) a
        LEFT JOIN (
            SELECT gift_key, if(gift_unit_val<=0,gift_gold/10,gift_unit_val) as gift_unit_val
            FROM default.tmp_hdfs_ingkee_gift_info
        ) d
        ON a.app_id = d.gift_key
        LEFT JOIN (
            SELECT gift_key, if(gift_unit_val<=0,gift_gold/10,gift_unit_val) as gift_unit_val
            FROM default.tmp_hdfs_ingkee_gift_info
        ) e
        ON a.app_name = e.gift_key;
     "

    echo "${tmp_ingkee_tbl_ex_live_p3_gift_danmu_data}"

    echo "########## 礼物弹幕不去重  #######"
    save_tbl_ex_live_gift_danmu_data_heavy="INSERT INTO TABLE live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot_heavy PARTITION(dt='${day}')
    select dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id
        , audience_id, audience_name, gift_id, gift_type, gift_name
        , gift_image_url, gift_num, content, gift_unit_price, type
        , gift_type_id, gift_unit_val, gift_val, hour
    from default.tmp_ingkee_tbl_ex_live_p3_gift_danmu_data
    ;
    "

    echo "########## 礼物弹幕去重  #######"
    save_tbl_ex_live_gift_danmu_data="INSERT INTO TABLE live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot PARTITION(dt='${day}')
    select dataSource, record_time, trace_id, schema, client_time
        , cloudServiceId, spiderVersion, appVersion, containerId, resourceKey
        , dataType, data_generate_time, appPackageName, room_id,
        if(substr(user_id,1,1)='@',substr(user_id,4),if(substr(user_id,1,1)=' ',substr(user_id,2),user_id )) as user_id
        , audience_id, audience_name, gift_id, gift_type, gift_name
        , gift_image_url, gift_num, content, gift_unit_price, type
        , gift_type_id, gift_unit_val, gift_val, hour
    from(
        select *,row_number() over (partition by appPackageName,user_id,audience_id,gift_name,gift_id,substr(data_generate_time,1,10) order by data_generate_time desc) as order_num
        from default.tmp_ingkee_tbl_ex_live_p3_gift_danmu_data
    ) as a
    where a.order_num=1;
    "

    delete_hive_partition="
    ALTER TABLE live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot_heavy DROP IF EXISTS PARTITION (dt='${day}');
    ALTER TABLE live_p3.tbl_ex_live_gift_danmu_data_daily_snapshot DROP IF EXISTS PARTITION (dt='${day}');
    "

    hdfs dfs -rm -r /data/ias_p3/live/snapshot/tbl_ex_live_gift_danmu_data_daily_snapshot_heavy/dt=${day}
    hdfs dfs -rm -r /data/ias_p3/live/snapshot/tbl_ex_live_gift_danmu_data_daily_snapshot/dt=${day}


    executeHiveCommand "
    ${delete_hive_partition}
    ${tmp_hdfs_ingkee_gift_info}
    ${tmp_ingkee_tbl_ex_live_p3_gift_danmu_data}
    ${save_tbl_ex_live_gift_danmu_data_heavy}
    ${save_tbl_ex_live_gift_danmu_data}
    "

echo "############### 礼物弹幕快照表 end #####################"


