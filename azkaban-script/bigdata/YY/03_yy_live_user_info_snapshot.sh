#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
#source /home/hadoop/yy/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_commen="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

#-----------------------------------------------
# 累计主播列表日临时表
#-----------------------------------------------

user_info_all="insert into table bigdata.yy_live_user_info_all_snapshot partition (dt='${date}')
    select
        update_date,user_id,user_name,live_desc,user_age,user_sex,user_label_list,
        share_url,location,fans_num,room_id,follow_num,user_family,user_integral,last_start_time,
        user_level,is_live,online_num,
        contribution_score,sold_in_month_count,classfication,
        business,data_generate_time,meta_table_name,meta_app_name
    from (
        select *,row_number() over (partition by user_id order by data_generate_time desc) as order_desc
        from
            (select
                dt update_date,
                user_id,user_name,live_desc,user_age,user_sex,user_label_list,
                share_url,location,
                if(fans_num is null,0, fans_num) fans_num,
                room_id,
                if(follow_num is null,0, follow_num) follow_num,
                user_family,user_integral,last_start_time,user_level,is_live,
                if(online_num is null,0,online_num) online_num,
                contribution_score,sold_in_month_count,classfication,
                business,data_generate_time,meta_table_name,meta_app_name
            from
                bigdata.YY_live_user_info_data_origin
            where
                dt = '${date}' and is_live = 1 and user_id is not null
            union all
            select
                update_date,user_id,user_name,live_desc,user_age,user_sex,user_label_list,
                share_url,location,fans_num,room_id,follow_num,user_family,user_integral,last_start_time,
                user_level,is_live,online_num,contribution_score,sold_in_month_count,classfication,
                business,data_generate_time,meta_table_name,meta_app_name
            from
                bigdata.yy_live_user_info_all_snapshot
            where dt = '${yesterday}'
            ) as a
        ) as t
    where
        t.order_desc = 1;"

#------------------------------------------
#-- 日主播数据表
#-- user_id 为直播间id
#------------------------------------------

user_info_daily="insert into table bigdata.yy_live_user_info_daily_snapshot partition (dt='${date}')
select
    a.user_id,a.user_name,a.start_time,a.end_time,a.duration,
    if(b.audience_num is null,0,b.audience_num) audience_num,
    if(c.gift_num is null,0,c.gift_num) gift_num,
    if(c.gift_val is null,0,c.gift_val) gift_val,
    'user_info_daily' as meta_table_name,
    'com.duowan.mobile' as meta_app_name
from bigdata.YY_live_broadcast_info_tmp a
left join
    bigdata.yy_user_audience_num_tmp b
on a.room_id = b.user_id
left join
    (select
        user_id,count(audience_id) gift_num, sum(gift_val) gift_val
    from
        bigdata.yy_gift_info_all_tmp
    group by user_id) c
on a.room_id = c.user_id;"


# ------------------------------------
# -- ES live_id_list 用于计算主播数量
# ------------------------------------
live_id_list="insert into table bigdata.yy_live_id_list_all_snapshot partition (dt='${date}')
select update_date,user_id,user_name,live_desc,data_generate_time,meta_table_name,meta_app_name
from (
    select *,row_number() over (partition by user_id order by data_generate_time desc) as order_desc
    from (
        select
            dt update_date,user_id,user_name,live_desc,data_generate_time,meta_table_name,meta_app_name
        from bigdata.yy_live_id_list_data_origin
        where dt='${date}' and user_id is not null
    union all
        select
            update_date,user_id,user_name,live_desc,data_generate_time,meta_table_name,meta_app_name
        from bigdata.yy_live_id_list_all_snapshot
        where dt='${yesterday}') a
    ) t
where t.order_desc=1;"


echo -n "主播表数据写入... "
executeHiveCommand "${user_info_all} ${user_info_daily} ${live_id_list}"
echo "主播表数据写入,OK"


# ----------------#
# 累计主播数据同步ES
# ----------------#

user_info_to_es="insert into table bigdata.yy_es_live_user_info_all_snapshot
    select
        update_date,user_id,concat(user_id,'-user_info_all-',dt) es_id,
        user_name,live_desc,user_age,user_sex,user_label_list,
        share_url,location,fans_num,room_id,follow_num,user_family,user_integral,last_start_time,
        user_level,is_live,online_num,
        contribution_score,sold_in_month_count,classfication,
        business,data_generate_time,meta_table_name,meta_app_name,dt,substr(dt,0,7) months
    from bigdata.yy_live_user_info_all_snapshot
    where dt='${date}';"

# ------------------- +
# 日主播数据同步 ES
# --------------------+

user_info_daily_to_es="insert into table bigdata.yy_es_live_user_info_daily_snapshot
    select concat(user_id,'-user_info_daily-',dt) es_id,
        user_id,user_name,start_time,end_time,duration,
        audience_num,gift_num,gift_val,
        meta_table_name,meta_app_name,dt,substr(dt,0,7) months
    from bigdata.yy_live_user_info_daily_snapshot
    where dt='${date}';"


# ------------------------ +
# live_id_list主播数同步 ES
# ------------------------ +

live_id_list_to_es="insert into table bigdata.yy_es_live_id_list_all_snapshot
    select
        update_date,
        concat(user_id,'-live_id_list-',dt) es_id,user_id,user_name,live_desc,
        meta_table_name,meta_app_name,dt,substr(dt,0,7) months
    from bigdata.yy_live_id_list_all_snapshot
    where dt='${date}';"

echo -n "主播表同步到ES... "
executeHiveCommand "${add_es_hadoop} ${add_commen} ${user_info_to_es} ${user_info_daily_to_es} ${live_id_list_to_es}"
echo "主播表同步到ES,OK"



