#!/bin/sh
source /etc/profile
# source /home/hadoop/yy/env.conf
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh


date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_commen="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

# 累计礼物列表，插入分区表 每天
hive_sql="insert into table bigdata.yy_live_gift_info_all_snapshot partition (dt='${date}')
select
    update_date,gift_id,if(gift_gold is null,0, gift_gold) gift_gold,gift_name,
    gift_image,data_generate_time,meta_table_name,meta_app_name
from (
    select *,row_number() over (partition by gift_id order by data_generate_time desc) as order_num
    from (
        select
            dt update_date,
            gift_id,gift_gold,gift_name,gift_image,data_generate_time,meta_table_name,meta_app_name
        from
            bigdata.YY_live_gift_info_data_origin
        where
            dt = '${date}' and gift_id is not null
        union all
        select
            update_date,gift_id,gift_gold,gift_name,gift_image,data_generate_time,meta_table_name,meta_app_name
        from
            bigdata.yy_live_gift_info_all_snapshot
        where dt='${yesterday}'
            ) as p
     ) as t
where t.order_num = 1;"

echo -n "累计礼物数据导入... "
executeHiveCommand "${hive_sql}"
echo "累计礼物数据导入,OK"

# --------------
# ES 同步
# --------------
to_es="insert into table bigdata.yy_es_live_gift_info_all_snapshot
    select
        update_date,concat(gift_id,'-live_gift_info-',dt) es_id,
        gift_id,gift_gold,gift_name,gift_image,
        data_generate_time,meta_table_name,meta_app_name,dt,
        substr(dt,0,7) months
    from
        bigdata.yy_live_gift_info_all_snapshot
    where
        dt='${date}' and gift_id is not null;"

echo -n "同步到ES, BEGIN..."
executeHiveCommand "${add_es_hadoop} ${add_commen} ${to_es}"
echo "同步到ES, OK"