#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

interval=5000
date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

# =========================================== 打赏金额计算 ================================================ #

# ----------------------------------------- #
# -- 挑出含礼物的弹幕
# ----------------------------------------- #
danmu_daily0="create temporary table bigdata.bili_danmu_gift_info_daily_tmp0 as
    select *
    from bigdata.bili_live_danmu_data_origin
    where dt='${date}' and gift_num>0 and room_id!='unknown'
        and audience_id is not null and audience_id!='';"

# --------------------------------------------#
# 末尾添加一条大于时间间隔的数据
#---------------------------------------------#
danmu_daily1="create temporary table bigdata.bili_danmu_gift_info_daily_tmp1 as
select
    room_id,audience_id,gift_id,gift_num,data_generate_time
from bigdata.bili_danmu_gift_info_daily_tmp0
union all
select
    room_id,audience_id,gift_id,0 as gift_num,
    (max(data_generate_time)+${interval}+1000) as data_generate_time
from bigdata.bili_danmu_gift_info_daily_tmp0
group by room_id,audience_id,gift_id;"

# --------------------------------------------- #
# -- 挑出时间间隔大于阈值的数据
# --------------------------------------------- #

danmu_daily2="create temporary table bigdata.bili_danmu_gift_info_daily_tmp2 as
    select a.room_id,a.audience_id,a.gift_id,
    a.gift_num a_gift_num,b.gift_num b_gift_num,
    a.data_generate_time a_data_generate_time,b.data_generate_time b_data_generate_time,
    case
        when b.data_generate_time-a.data_generate_time<${interval} and a.gift_num<b.gift_num
        then 0
    else 1
    end tag
    from
    (select
        room_id,audience_id,gift_id,gift_num,data_generate_time,
        row_number() over(partition by room_id,audience_id,gift_id order by data_generate_time asc) row_id
    from bigdata.bili_danmu_gift_info_daily_tmp1) a
    join
    (select
        room_id,audience_id,gift_id,gift_num,data_generate_time,
        row_number() over(partition by room_id,audience_id,gift_id order by data_generate_time asc) row_id
    from bigdata.bili_danmu_gift_info_daily_tmp1) b
    on a.row_id=b.row_id-1 and a.room_id=b.room_id and a.audience_id=b.audience_id and a.gift_id=b.gift_id;"

# ------------------------------ #
# -- 当天礼物收入
# ------------------------------ #
payer_info_all="create table bigdata.bili_payer_gift_info_tmp as
    select
        a.room_id,a.audience_id,c.audience_name,
        c.audience_title,c.audience_title_grade,c.audience_level,
        a.gift_id,a.gift_num,
        if(b.gift_gold is null,0,b.gift_gold) gift_gold,
        if((a.gift_num * b.gift_gold) is null,0,a.gift_num * b.gift_gold) gift_val
    from (
        select room_id,audience_id,gift_id,
            sum(a_gift_num) gift_num
        from bigdata.bili_danmu_gift_info_daily_tmp2
        where tag=1
        group by room_id,audience_id,gift_id) a
    left join (
        select * from(
            select *,row_number() over(partition by room_id,audience_id,gift_id order by data_generate_time desc) row_id
            from bigdata.bili_danmu_gift_info_daily_tmp0) d
        where d.row_id=1
        ) c
     on a.room_id=c.room_id and a.audience_id=c.audience_id and a.gift_id=c.gift_id
     left join (
        select gift_id,gift_gold
        from bigdata.bili_live_gift_info_all_snapshot
        where dt='${date}') b
    on a.gift_id=b.gift_id;"

# ============================================ 大航海价值计算 ================================================ #

# -- 大航海数量计算 -- #
guard_num="create temporary table bigdata.bili_live_guard_num_tmp as
select a.room_id,a.guard_num
from(
    select *,row_number() over(partition by room_id order by data_generate_time desc) row_id
    from bigdata.bili_live_guard_num_data_origin
    where dt='${date}' and guard_num>0) a
where a.row_id=1;"

# -- 大航海明细表计算 -- #
guard_list="create table bigdata.bili_live_guard_list_value_tmp as
    select
        a.room_id,a.guard_user_id payer_id,a.guard_user_name payer_name,
        a.guard_level,a.guard_rank,b.guard_val,
        a.data_generate_time,a.meta_table_name,a.meta_app_name,dt
    from(
        select * from (
            select *,row_number() over(partition by guard_user_id,room_id order by data_generate_time desc) row_id
            from bigdata.bili_live_guard_list_data_origin
            where dt='${date}' and room_id!='unknown'
                and guard_user_id is not null and guard_user_id!='') c
        where c.row_id=1) a
    join bigdata.bili_live_guard_value_data_origin b
    on a.guard_level=b.guard_level;"

# -- 大航海统计临时表 -- #
guard_stat="create temporary table bigdata.bili_live_guard_list_stat_tmp as
select
    if(a.room_id is null,b.room_id,a.room_id) room_id,
    if(b.room_id is null,0,b.guard_num) guard_num1,
    if(a.room_id is null,0,a.guard_num) guard_num2,
    if(b.room_id is null,0,zong_du) zong_du,
    if(b.room_id is null,0,ti_du) ti_du,
    if(b.room_id is null,0,jian_zhang) jian_zhang1
from bigdata.bili_live_guard_num_tmp a
full join
    (select room_id,count(payer_id) guard_num,
        count(case when guard_level=1 then payer_id else null end) zong_du,
        count(case when guard_level=2 then payer_id else null end) ti_du,
        count(case when guard_level=3 then payer_id else null end) jian_zhang
    from bigdata.bili_live_guard_list_value_tmp
    group by room_id) b
on a.room_id=b.room_id;"

# -- 大航海价值统计临时表 -- #
guard_value="create table bigdata.bili_live_guard_stat_value_tmp as
select
     a.room_id,a.guard_num1,a.guard_num2,
     a.zong_du,a.ti_du,a.jian_zhang1,a.jian_zhang2,
     a.zong_du*b.guard_val+a.ti_du*c.guard_val+a.jian_zhang2*d.guard_val guard_val
from
    (select *,
    if(guard_num1<guard_num2,guard_num2-zong_du-ti_du,jian_zhang1) jian_zhang2
    from bigdata.bili_live_guard_list_stat_tmp) a,
    (select guard_val from bigdata.bili_live_guard_value_data_origin where guard_level=1) b,
    (select guard_val from bigdata.bili_live_guard_value_data_origin where guard_level=2) c,
    (select guard_val from bigdata.bili_live_guard_value_data_origin where guard_level=3) d;"


# ============================================ 互动人数计算 ============================================= #

audience_num="create table bigdata.bili_user_audience_num_tmp AS
    select
        a.room_id,count(audience_id) audience_num
    from (
        select room_id,audience_id
        from bigdata.bili_live_danmu_data_origin
        where dt='${date}' and room_id!='unknown'
            and audience_id is not null and audience_id!=''
        group by room_id,audience_id
        ) a
    group by a.room_id;"


# ====================================== 计算开播时间与开播时长 BEGIN ========================================== #

# ----------------------------------------#
# 每个主播最大最小数据
# 第一条开播的数据为准
# 最后一条数据无论是否是开播
# ----------------------------------------#
bc_outside="create temporary table bigdata.bili_broadcast_outside_tmp as
    select room_id,min(data_generate_time) start_time,
    min(data_generate_time) end_time,
    1 from_state,1 to_state
    from bigdata.bili_live_user_info_data_origin
    where dt='${date}' and is_live=1 and room_id!='unknown'
    group by room_id
union all
    select room_id,max(data_generate_time) start_time,
    max(data_generate_time) end_time,
    0 from_state,0 to_state
    from bigdata.bili_live_user_info_data_origin
    where dt='${date}' and room_id!='unknown'
    group by room_id;"

# ------------------------ #
# 原始数据添加序号
# ------------------------ #
bc_row_num="create temporary table bigdata.bili_broadcast_row_num_tmp as
    select row_number() over(partition by room_id order by data_generate_time asc) row_id,
        room_id,data_generate_time,is_live
    from bigdata.bili_live_user_info_data_origin
    where dt='${date}' and room_id!='unknown';"

# ----------------------- #
# 获取中间数据--join
# ----------------------- #
bc_row_mid="create temporary table bigdata.bili_broadcast_row_middle_tmp as
    select a.row_id,a.room_id,a.data_generate_time start_time,b.data_generate_time end_time,
    a.is_live from_state,b.is_live to_state
    from bigdata.bili_broadcast_row_num_tmp a
    join bigdata.bili_broadcast_row_num_tmp b
    on a.row_id = b.row_id-1 and a.room_id=b.room_id;"


# ---------------------------------------- #
# 筛选开始结尾及中间状态变换的数据
# ---------------------------------------- #
bc_row_exchange="create temporary table bigdata.bili_broadcast_row_exchange_tmp as
    select row_id,room_id,
    case
        when from_state=0 and to_state=1
        then end_time
        when from_state=1 and to_state=1
        then start_time
    end start_time,
    case
        when from_state=1 and to_state=0
        then end_time
        when from_state=0 and to_state=0
        then start_time
    end end_time,from_state,to_state
    from (
        select row_number() over(partition by room_id order by start_time,end_time asc) row_id,*
        from(
        select room_id,start_time,end_time,from_state,to_state
        from bigdata.bili_broadcast_row_middle_tmp
        where from_state!=to_state
        union all
        select room_id,start_time,end_time,from_state,to_state
        from bigdata.bili_broadcast_outside_tmp) a
    ) b;"

# ---------------------------------------- #
# 最近结果临时表
# ---------------------------------------- #
bc_final="create table bigdata.bili_broadcast_row_final_tmp as
    select a.room_id,a.start_time,b.end_time,
        b.end_time-a.start_time duration
    from bigdata.bili_broadcast_row_exchange_tmp a
    join bigdata.bili_broadcast_row_exchange_tmp b
    on a.row_id=b.row_id-1 and a.room_id=b.room_id
    where a.start_time is not null and b.end_time is not null;"

# ====================================== 计算开播时间与开播时长 END ========================================== #

echo "日期：${date}"
echo "打赏用户临时表创建, BEGIN..."
executeHiveCommand "${danmu_daily0} ${danmu_daily1} ${danmu_daily2} ${payer_info_all}"
# /usr/bin/hive -e  "${danmu_daily0} ${danmu_daily1} ${danmu_daily2} ${payer_info_all}"
echo "打赏用户临时表创建, END."

echo "大航海用户价值计算, BEGIN..."
executeHiveCommand "${guard_num} ${guard_list} ${guard_stat} ${guard_value}"
# /usr/bin/hive -e  "${guard_num} ${guard_list} ${guard_stat} ${guard_value}"
echo "大航海用户价值计算, END."

echo "互动人数临时表创建, BEGIN..."
executeHiveCommand "${audience_num}"
# /usr/bin/hive -e  "${audience_num}"
echo "互动人数临时表创建, END."

echo "开播时间与开播时长临时表创建, BEGIN..."
executeHiveCommand "${bc_outside} ${bc_row_num} ${bc_row_mid} ${bc_row_exchange} ${bc_final}"
# /usr/bin/hive -e  "${bc_outside} ${bc_row_num} ${bc_row_mid} ${bc_row_exchange} ${bc_final}"
echo  "开播时间与开播时长临时表创建, END."