#!/bin/sh
source /etc/profile
#source /home/hadoop/yy/env.conf
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
last_week=`date -d "-7 day $date" +%Y-%m-%d`
last_month=`date -d "-30 day $date" +%Y-%m-%d`
months=`date -d "$date" +%Y-%m`

add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_commen="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

# ---------------------------------------------- #
# -- hive 平台数据统计表
# ---------------------------------------------- #
bili_stat="insert overwrite table bigdata.bili_live_platform_statistics_snapshot
select a.online_num,b.audience_num,c.bc_num,d.bc_length,
    cast(d.bc_length/c.bc_num as bigint) bc_avg,
    e.t_active_live_num,e1.t_new_live_num,f.w_active_live_num,g.m_active_live_num,
    h.da_hang_hai_num,h1.guard_num,i.t_active_reward_num,j.t_new_reward_num,
    k.w_active_reward_num,l.m_active_reward_num,m.reward_val,
    cast(m.reward_val/1000 as bigint) reward_rmb,
    cast(n.guard_val/1000 as bigint) guard_rmb,
    'platform_statistics' meta_table_name,
    'bili' meta_app_name,
    '${date}' dt
from
    (select
        max(online_num) online_num -- 最高人气值
    from(
        select data_generate_time,sum(online_num) online_num
        from bigdata.bili_live_user_info_data_origin
        where dt='${date}'
        group by data_generate_time
    ) a1) a ,
    (select
        count(1) as audience_num  -- 日互动人数
    from(
        select room_id,audience_id
        from bigdata.bili_live_danmu_data_origin
        where dt='${date}'
        group by room_id,audience_id
    ) b1) b,
    (select
        count(1) as bc_num -- 日开播次数
    from (
        select room_id,start_time
        from bigdata.bili_live_streamer_info_daily_snapshot
        where dt='${date}' and duration>0
    ) c1) c,
    (select
        cast(sum(duration)/60000 as bigint) as bc_length --日开播总时长
    from bigdata.bili_live_streamer_info_daily_snapshot
    where dt='${date}' and duration>0) d,
    (select
        count(1) as t_active_live_num  -- 日活跃主播数
    from bigdata.bili_live_user_info_all_snapshot
    where dt='${date}' and update_date='${date}') e,
    (select
        count(1) as t_new_live_num -- 日新增主播数
    from bigdata.bili_live_user_info_all_snapshot
    where dt='${date}' and registration='${date}'
    ) e1,
    (select
        count(1) as w_active_live_num -- 周活跃主播数
    from bigdata.bili_live_user_info_all_snapshot
    where dt='${date}' and update_date>='${last_week}') f,
    (select
        count(1) as m_active_live_num --月活跃主播数
    from bigdata.bili_live_user_info_all_snapshot
    where dt='${date}' and update_date>='${last_month}') g,
    (select
        count(1) as da_hang_hai_num --日大航海用户数
    from bigdata.bili_live_guard_list_data_origin
    where dt='${date}') h,
    (select
        sum(if(guard_num1>guard_num2,guard_num1,guard_num2)) guard_num --日大航海用户数
    from bigdata.bili_live_streamer_income_daily_snapshot
    where dt='${date}') h1,
    (select
        count(1) as t_active_reward_num  -- 日活跃打赏用户数
    from bigdata.bili_live_payer_info_all_snapshot
    where dt='${date}' and last_reward_date='${date}' and pay_type!=2) i,
    (select
        count(1) as t_new_reward_num -- 日新增打赏用户数
    from bigdata.bili_live_payer_info_all_snapshot
    where dt='${date}' and registration='${date}' and pay_type!=2) j,
    (select
        count(1) as w_active_reward_num --周活跃打赏用户数
    from bigdata.bili_live_payer_info_all_snapshot
    where dt='${date}' and last_reward_date>='${last_week}'and pay_type!=2) k,
    (select
        count(1) as m_active_reward_num  --月活跃打赏用户数
    from bigdata.bili_live_payer_info_all_snapshot
    where dt='${date}' and last_reward_date>='${last_month}' and pay_type!=2) l,
    (select
        sum(gift_val) as reward_val --日平台打赏金瓜子数
    from bigdata.bili_live_streamer_income_daily_snapshot
    where dt='${date}') m,
    (select
        sum(guard_val) as guard_val --日平台大航海总金额
    from bigdata.bili_live_streamer_income_daily_snapshot
    where dt='${date}') n
    union all
    select
        online_num,audience_num,bc_num,bc_length,bc_avg,
        t_active_live_num,t_new_live_num,w_active_live_num,m_active_live_num,
        da_hang_hai_num,guard_num,t_active_reward_num,t_new_reward_num,
        w_active_reward_num,m_active_reward_num,reward_val,
        reward_rmb,guard_rmb,meta_table_name,meta_app_name,dt
    from bigdata.bili_live_platform_statistics_snapshot;"


# ------------------------------------- #
# -- ES写入
# ------------------------------------- #
bili_stat_es="insert into table bigdata.bili_es_live_platform_statistics_snapshot
    select
    concat('platform_statistics-',dt) es_id,*,
    '${months}' months
    from bigdata.bili_live_platform_statistics_snapshot
    where dt='${date}';"


# ================================================ 函数执行 ================================================== #

echo "最终统计表计算, BEGIN..."
executeHiveCommand "${bili_stat}"
# /usr/bin/hive -e "${bili_stat}"
echo "最终统计表完成, END."

echo "最终统计写入ES, BEGIN..."
executeHiveCommand "${add_es_hadoop} ${add_commen}  ${bili_stat_es}"
# /usr/bin/hive -e "${add_es_hadoop} ${add_commen}  ${bili_stat_es}"
echo "最终统计写入ES, END."
