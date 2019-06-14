#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
#source /home/hadoop/yy/env.conf
source ${base_path}/util.sh


date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

# --------------------------------------------#
# live gift user info temporary 打赏用户临时表
# 下游依赖:
# 日付费用户
# 累计付费用户
# 此处user_id 为room_id
#----------------------------------------------#
yy_gift_info_all_tmp="create table bigdata.yy_gift_info_all_tmp as
    select
        a.user_id,audience_id,a.gift_id,a.gift_num, -- gift_num 每个观众送的礼物数
        if(b.gift_gold is null,0,b.gift_gold) gift_gold,
        if((a.gift_num * b.gift_gold) is null,0,a.gift_num * b.gift_gold) gift_val,
        1 as pay_type -- 付费类型
    from (
        select user_id,audience_id,gift_id,sum(gift_num) gift_num
        from bigdata.YY_live_danmu_data_origin
        where dt='${date}' and gift_num > 0
              and user_id is not null and audience_id is not null and gift_id is not null
        group by user_id,audience_id,gift_id
        ) a
    left join
        (select gift_id,gift_gold
        from bigdata.yy_live_gift_info_all_snapshot
        where dt='${date}') b
    on a.gift_id = b.gift_id;"

# ---------------------------------#
# 主播互动人数统计
# user_id is room_id
#----------------------------------#
yy_user_audience_num_tmp="create table bigdata.yy_user_audience_num_tmp AS
    select
        a.user_id,count(audience_id) audience_num
    from (
        select user_id,audience_id
        from bigdata.YY_live_danmu_data_origin
        where dt='${date}' and user_id is not null and audience_id is not null
        group by user_id,audience_id
        ) a
    group by a.user_id;"


# ----------------------------------------#
# 直播开始时间，结束时间，直播时长
# ----------------------------------------#
YY_live_broadcast_info_tmp="create table bigdata.YY_live_broadcast_info_tmp as
    select user_id,user_name,room_id,start_time,end_time,
            if((end_time=0 or start_time=0), 0,(end_time - start_time)) duration
    from(
        select user_id,user_name,room_id,start_time,end_time
        from (
            select user_id,user_name,room_id,
                if(start_time is null,0,start_time) start_time,
                floor(data_generate_time/1000) end_time,
                row_number () over (partition by user_id,start_time order by data_generate_time desc) rowid
            from bigdata.yy_live_id_list_data_origin
            where dt='${date}' and user_id is not null
        ) t
        where t.rowid = 1
    ) a;"
echo -n "临时表创建, Begin..."
executeHiveCommand "${yy_gift_info_all_tmp} ${yy_user_audience_num_tmp} ${YY_live_broadcast_info_tmp}"
echo "OK"