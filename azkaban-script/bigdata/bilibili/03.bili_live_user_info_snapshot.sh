#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_commen="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

# -------------------------- #
# 累计主播列表日临时表
# -------------------------- #
user_tmp="create temporary table bigdata.bili_live_user_info_all_tmp as
    select
        a.update_date,if(b.registration is null,a.dt,b.registration) registration,
        a.room_id,a.user_name,a.user_level,
        a.area_name,a.fans_num,a.online_num,
        a.data_generate_time,a.meta_table_name,a.meta_app_name,a.dt
    from (
        select *
        from(
            select
                row_number() over (partition by room_id order by data_generate_time desc) as order_desc,
                dt update_date,
                room_id,user_name,user_level,area_name,
                if(fans_num is null,0,fans_num) fans_num,
                if(online_num is null,0,online_num) online_num,
                data_generate_time,meta_table_name,meta_app_name,dt
            from
                bigdata.bili_live_user_info_data_origin
            where
                dt='${date}' and is_live=1 and room_id!='unknown'
            ) c
        where c.order_desc=1
    ) a
    left join (
        select room_id,registration
        from bigdata.bili_live_user_info_all_snapshot
        where dt='${yesterday}'
    ) b
    on a.room_id = b.room_id;"

# ---------------------- #
# 添加新增主播时间
# ---------------------- #
user_info_all="insert into table bigdata.bili_live_user_info_all_snapshot partition (dt='${date}')
    select
    update_date,registration,room_id,user_name,user_level,
    area_name,fans_num,online_num,
    data_generate_time,meta_table_name,meta_app_name
    from (
        select *,row_number() over (partition by room_id order by data_generate_time desc) as order_desc
        from
            (select
                dt update_date,registration,
                room_id,user_name,user_level,area_name,
                if(fans_num is null,0,fans_num) fans_num,
                if(online_num is null,0,online_num) online_num,
                data_generate_time,meta_table_name,meta_app_name,dt
            from
                bigdata.bili_live_user_info_all_tmp
            union all
            select
                update_date,registration,
                room_id,user_name,user_level,
                area_name,fans_num,online_num,
                data_generate_time,meta_table_name,meta_app_name,dt
            from
                bigdata.bili_live_user_info_all_snapshot
            where dt = '${yesterday}'
            ) as a
        ) as t
    where t.order_desc=1;"

# ======================================== 日主播数据表 ====================================================== #
#-- 日主播数据表 --#

user_info_daily="insert into table bigdata.bili_live_user_info_daily_snapshot partition (dt='${date}')
select
    a.room_id,d.user_name,d.user_level,d.area_name,d.fans_num,d.online_num,
    a.start_time,a.end_time,a.duration,
    if(b.audience_num is null,0,b.audience_num) audience_num,
    if(c.gift_num is null,0,c.gift_num) gift_num,
    if(c.gift_val is null,0,c.gift_val) gift_val,
    if(e.guard_val is null,0,e.guard_val) guard_val,
    'user_info_daily' as meta_table_name,
    d.meta_app_name
from bigdata.bili_broadcast_row_final_tmp a
left join
    bigdata.bili_user_audience_num_tmp b
on a.room_id = b.room_id
left join
    (select
        room_id,count(audience_id) gift_num,sum(gift_val) gift_val
    from
        bigdata.bili_payer_gift_info_tmp
    group by room_id) c
on a.room_id = c.room_id
left join
    (select room_id,user_name,user_level,area_name,fans_num,online_num,meta_app_name
    from bigdata.bili_live_user_info_all_snapshot
    where dt = '${date}') d
 on a.room_id=d.room_id
left join (
    select room_id,sum(guard_val) guard_val
    from bigdata.bili_live_guard_list_value_tmp
    where guard_val is not null
    group by room_id) e
 on a.room_id=e.room_id;"

#-- 日主播表直播信息表 --#
streamer_info="insert into table bigdata.bili_live_streamer_info_daily_snapshot partition (dt='${date}')
select
    a.room_id,d.user_name,d.user_level,d.area_name,d.fans_num,d.online_num,
    a.start_time,a.end_time,a.duration,
    'streamer_info' as meta_table_name,
    d.meta_app_name
from bigdata.bili_broadcast_row_final_tmp a
left join
    (select room_id,user_name,user_level,area_name,fans_num,online_num,meta_app_name
    from bigdata.bili_live_user_info_all_snapshot
    where dt = '${date}') d
 on a.room_id=d.room_id;"


#-- 日主播打赏临时表 --#
income_tmp="create temporary table bigdata.bili_live_streamer_income_daily_tmp as
select
    a.room_id,a.gift_num,a.gift_payer_num,a.gift_val,
    if(b.audience_num is null,0,b.audience_num) interact_user_num,
    if(c.bc_num is null,0,c.bc_num) bc_num,
    if(c.bc_avg is null,0,c.bc_avg) bc_avg
from (select
        room_id,sum(gift_num) gift_num,
        count(audience_id) gift_payer_num,
        sum(gift_val) gift_val
    from
        bigdata.bili_payer_gift_info_tmp
    group by room_id) a
left join
    bigdata.bili_user_audience_num_tmp b
on a.room_id = b.room_id
left join
    (select room_id,count(start_time) bc_num,
        cast(sum(duration)/count(start_time)/60000 as int) bc_avg
    from bigdata.bili_live_streamer_info_daily_snapshot
    where dt = '${date}'
    group by room_id) c
 on a.room_id=c.room_id;"

#-- 日主播收入合计表 --#
streamer_income="insert into table bigdata.bili_live_streamer_income_daily_snapshot partition (dt='${date}')
select
    if(a.room_id is null,b.room_id,a.room_id) room_id,
    if(a.room_id is null,0,a.gift_num) gift_num,
    if(a.room_id is null,0,a.gift_payer_num) gift_payer_num,
    if(a.room_id is null,0,a.gift_val) gift_val,
    if(a.room_id is null,0,a.interact_user_num) interact_user_num,
    if(a.room_id is null,0,a.bc_num) bc_num,
    if(a.room_id is null,0,a.bc_avg) bc_avg,
    if(b.guard_num1 is null,0,guard_num1) guard_num1,
    if(b.guard_num2 is null,0,guard_num2) guard_num2,
    if(b.guard_val is null,0,guard_val) guard_val,
    'streamer_income' as meta_table_name,
    'bili' meta_app_name
from bigdata.bili_live_streamer_income_daily_tmp a
full join bigdata.bili_live_guard_stat_value_tmp b
 on a.room_id=b.room_id;"


# ============================================= live_id_list ================================================ #

live_id_list="insert into table bigdata.bili_live_id_list_all_snapshot partition (dt='${date}')
select update_date,room_id,user_name,live_desc,
    area_id,area_name,parentAreaName,face_url,
    data_generate_time,meta_table_name,meta_app_name
from (
    select *,row_number() over (partition by room_id order by data_generate_time desc) as order_desc
    from (
        select
            dt update_date,room_id,user_name,live_desc,
            area_id,area_name,parentAreaName,face_url,
            data_generate_time,meta_table_name,meta_app_name
        from bigdata.bili_live_id_list_data_origin
        where dt='${date}' and room_id!='unknown'
    union all
        select
            update_date,room_id,user_name,live_desc,
            area_id,area_name,parentAreaName,face_url,
            data_generate_time,meta_table_name,meta_app_name
        from bigdata.bili_live_id_list_all_snapshot
        where dt='${yesterday}') a
    ) t
where t.order_desc=1;"

# ---------------- #
# 累计主播数据同步ES
# ---------------- #

user_info_to_es="insert into table bigdata.bili_es_live_user_info_all_snapshot
    select
        update_date,registration,
        concat(room_id,'-user_info_all-',dt) es_id,
        room_id,user_name,user_level,
        area_name,fans_num,online_num,
        data_generate_time,
        'user_info_all' meta_table_name,
        meta_app_name,dt,substr(dt,0,7) months
    from bigdata.bili_live_user_info_all_snapshot
    where dt='${date}';"

# ------------------- +
# 日主播数据同步 ES
# --------------------+

user_info_daily_to_es="insert into table bigdata.bili_es_live_user_info_daily_snapshot
    select concat(room_id,'-user_info_daily-',start_time) es_id,
        room_id,user_name,user_level,area_name,fans_num,online_num,
        start_time,end_time,duration,
        audience_num,gift_num,gift_val,guard_val,
        'user_info_daily' meta_table_name,
        meta_app_name,dt,substr(dt,0,7) months
    from bigdata.bili_live_user_info_daily_snapshot
    where dt='${date}';"

# ------------------------ +
# live_id_list主播数同步 ES
# ------------------------ +

live_id_list_to_es="insert into table bigdata.bili_es_live_id_list_all_snapshot
    select
        update_date,
        concat(room_id,'-live_id_list-',dt) es_id,
        room_id,user_name,live_desc,
        area_id,area_name,parentareaname,face_url,data_generate_time,
        'live_id_list' meta_table_name,
        meta_app_name,dt,substr(dt,0,7) months
    from bigdata.bili_live_id_list_all_snapshot
    where dt='${date}';"

# ================================================ 函数执行 ================================================== #

echo "日期：${date}"
echo "累计主播表计算, BEGIN... "
executeHiveCommand "${user_tmp} ${user_info_all}"
# /usr/bin/hive -e  "${user_tmp} ${user_info_all}"
echo "累计主播表计算, END."

echo "日主播明细表计算, BEGIN... "
executeHiveCommand "${user_info_daily}"
# /usr/bin/hive -e  "${user_info_daily}"
echo "日主播明细表计算, END."

echo "日主播开播数据明细表计算, BEGIN... "
executeHiveCommand "${streamer_info}"
# /usr/bin/hive -e  "${streamer_info}"
echo "日主播开播数据明细表计算, END."

echo "日主播收入合计表计算, BEGIN... "
executeHiveCommand "${income_tmp} ${streamer_income}"
#/usr/bin/hive -e  "${income_tmp} ${streamer_income}"
echo "日主播收入合计表计算, END."

echo "主播列表计算, BEGIN... "
executeHiveCommand "${live_id_list}"
# /usr/bin/hive -e  "${live_id_list}"
echo "主播列表计算, END."

echo -n "主播表同步到ES... "
executeHiveCommand "${add_es_hadoop} ${add_commen} ${user_info_to_es} ${user_info_daily_to_es} ${live_id_list_to_es}"
#/usr/bin/hive -e  "${add_es_hadoop} ${add_commen} ${user_info_to_es} ${user_info_daily_to_es} ${live_id_list_to_es}"
echo "主播表同步到ES,OK"



