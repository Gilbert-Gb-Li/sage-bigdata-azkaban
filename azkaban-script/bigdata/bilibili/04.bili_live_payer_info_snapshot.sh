#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_commen="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

# ---------------------------- #
# 计算当天付费用户数据
# ---------------------------- #
payer_info_today="create temporary table bigdata.bili_live_payer_info_today_tmp as
    select
        if(a.payer_id is null,b.payer_id,a.payer_id) payer_id,
        if(a.payer_id is null,b.payer_name,a.payer_name) payer_name,
        if(a.payer_id is null,'--',a.payer_level) payer_level,
        if(a.payer_id is null,'--',a.payer_title) payer_title,
        if(a.payer_id is null,'--',a.title_grade) title_grade,
        case
        when a.payer_id is not null and b.payer_id is null then 1
        when a.payer_id is null and b.payer_id is not null then 2
        when a.payer_id is not null and b.payer_id is not null then 3
        end pay_type,
        a.dt last_reward_date,
        b.dt last_guard_date,
        '${date}' dt
    from(
        select audience_id payer_id,audience_name payer_name,
            audience_level payer_level,
            audience_title payer_title,audience_title_grade title_grade,dt
        from(
        select *,row_number() over(partition by audience_id order by data_generate_time desc) row_id
        from bigdata.bili_live_danmu_data_origin
        where dt='${date}' and gift_num > 0 and room_id!='unknown'
            and audience_id is not null and audience_id!='') c
        where c.row_id=1) a
    full join (
        select payer_id,payer_name,dt
        from bigdata.bili_live_guard_list_value_tmp
        group by payer_id,payer_name,dt) b
    on a.payer_id=b.payer_id;"


# -------------------------- #
# 创建临时表存储合并后的数据
# -------------------------- #
payer_info_tmp="create temporary table bigdata.bili_live_payer_info_all_tmp as
    select a.payer_id,a.payer_name,
        a.payer_level,a.payer_title,a.title_grade,
        case
        when a.pay_type=3 or b.pay_type=3 then 3
        when a.pay_type=1 and b.pay_type=1 then 1
        when a.pay_type=1 and b.pay_type=2 then 3
        when a.pay_type=2 and b.pay_type=1 then 3
        when a.pay_type=2 and b.pay_type=2 then 2
        else a.pay_type
        end pay_type,
        if(b.registration is null,a.dt,registration) registration,
        if(a.last_reward_date is null,b.last_reward_date,a.last_reward_date) last_reward_date,
        if(a.last_guard_date is null,b.last_guard_date,a.last_guard_date) last_guard_date,
        a.dt
    from(
        select
            payer_id,payer_name,payer_level,payer_title,
            title_grade,pay_type,last_reward_date,last_guard_date,dt
        from bigdata.bili_live_payer_info_today_tmp) a
    left join (
        select
            registration,payer_id,pay_type,last_reward_date,last_guard_date
        from bigdata.bili_live_payer_info_all_snapshot
        where dt='${yesterday}') b
    on a.payer_id=b.payer_id;"


#----------------------------
#-- 累计付费用户数据，全量按天分区
#-- 送礼数量，礼物价值，每天数值不同且非递增，添加在此表中没有意义
#----------------------------
payer_info_all="insert into table bigdata.bili_live_payer_info_all_snapshot partition (dt='${date}')
    select
        update_date,registration,
        payer_id,payer_name,payer_level,
        payer_title,title_grade,
        'payer_info_all' as meta_table_name,
        'bili' as meta_app_name,
        pay_type,last_reward_date,last_guard_date
    from (
        select *,row_number() over(partition by payer_id order by update_date desc) row_id
        from (
            select dt update_date,registration,payer_id,
                payer_name,payer_level,payer_title,title_grade,
                pay_type,last_reward_date,last_guard_date
            from bigdata.bili_live_payer_info_all_tmp
            union all
            select update_date,registration,payer_id,
                payer_name,payer_level,payer_title,title_grade,
                pay_type,last_reward_date,last_guard_date
            from bigdata.bili_live_payer_info_all_snapshot
            where dt='${yesterday}') a
        ) t
    where t.row_id=1;"


# ====================================== 付费用户日明细表计算 ============================================ #

payer_info_daily="insert into table bigdata.bili_live_payer_info_daily_snapshot partition (dt='${date}')
    select
        if(a.audience_id is null,b.payer_id,a.audience_id) payer_id,
        if(a.audience_id is null,b.room_id,a.room_id) room_id,
        if(a.audience_id is null,b.payer_name,a.audience_name) payer_name,
        if(a.audience_level is null,'--',a.audience_level) payer_level,
        if(a.audience_title is null,'--',a.audience_title) payer_title,
        if(a.audience_title_grade is null,'--',a.audience_title_grade) title_grade,
        if(b.guard_level is null,'--',b.guard_level) guard_level,
        if(b.guard_rank is null,'--',b.guard_rank) guard_rank,
        if(a.gift_id is null,'--',a.gift_id) gift_id,
        if(a.gift_num is null,0,a.gift_num) gift_num,
        if(a.gift_val is null,0,a.gift_val) gift_val,
        if(b.guard_val is null,0,b.guard_val) guard_val,
        case
        when a.audience_id is not null and b.payer_id is null
        then 1
        when a.audience_id is null and b.payer_id is not null
        then 2
        when a.audience_id is not null and b.payer_id is not null
        then 3
        end pay_type,
        'payer_info_daily' as meta_table_name,
        'bili' as meta_app_name
    from
        bigdata.bili_payer_gift_info_tmp a
    full join
        bigdata.bili_live_guard_list_value_tmp b
    on a.audience_id=b.payer_id and a.room_id=b.room_id;"

payment="insert into table bigdata.bili_live_payer_payment_daily_snapshot partition (dt='${date}')
select
    a.payer_id,
    a.payer_name,
    sum(a.gift_val) gift_val,
    sum(a.guard_val) guard_val,
    a.pay_type,
    'payment_daily' meta_table_name,
    'bili' meta_app_name
from (
    select
        payer_id,payer_name,
        sum(gift_val) gift_val,
        guard_val,pay_type
    from bigdata.bili_live_payer_info_daily_snapshot
    where dt='${date}'
    group by payer_id,payer_name,room_id,guard_val,pay_type) a
group by a.payer_id,a.payer_name,a.pay_type;"


# ---------------------- +
# 日付费用户数据同步ES
# ---------------------- +
payer_daily_to_es="insert into table bigdata.bili_es_live_payer_info_daily_snapshot
     select concat(payer_id,'-',room_id,'-',gift_id,'-user_info_daily-',dt) es_id,
        payer_id,room_id,payer_name,payer_level,
        payer_title,title_grade,guard_level,guard_rank,
        gift_id,gift_num,gift_val,guard_val,pay_type,
        'payer_info_daily' as meta_table_name,
        'com.duowan.mobile' as meta_app_name,
        dt,substr(dt,0,7) months
     from bigdata.bili_live_payer_info_daily_snapshot
     where dt='${date}';"

# ---------------------- +
# 累计付费用户数据同步ES
# ---------------------- +
payer_info_all_to_es="insert into table bigdata.bili_es_live_payer_info_all_snapshot
    select update_date,registration,
        concat(payer_id,'-user_info_all-',dt) es_id,
        payer_id,payer_name,payer_level,payer_title,title_grade,
        'payer_info_all' as meta_table_name,
        'com.duowan.mobile' as meta_app_name,
        dt,substr(dt,0,7) months
from  bigdata.bili_live_payer_info_all_snapshot
    where dt='${date}';"

# ================================================ 函数执行 ================================================== #

# -- 中间表计算 -- #
echo "日期：${date}"
echo "累计付费用户数据计算, BEGIN..."
executeHiveCommand "${payer_info_today} ${payer_info_tmp} ${payer_info_all}"
# /usr/bin/hive -e  "${payer_info_today} ${payer_info_tmp} ${payer_info_all}"
echo "累计付费用户数据计算, END."

echo "付费用户日明细表计算, BEGIN..."
executeHiveCommand "${payer_info_daily}"
# /usr/bin/hive -e  "${payer_info_daily}"
echo "付费用户日明细表计算, END."

echo "付费用户表合计计算, BEGIN..."
executeHiveCommand "${payment}"
# /usr/bin/hive -e "${payment}"
echo "付费用户表合计计算, END."

# -- 同步ES -- #
echo -n "同步到ES... "
executeHiveCommand "${add_es_hadoop} ${add_commen} ${payer_daily_to_es} ${payer_info_all_to_es}"
# /usr/bin/hive -e "${add_es_hadoop} ${add_commen} ${payer_daily_to_es} ${payer_info_all_to_es}"
echo "同步到ES,OK"

# -- 删除临时表 -- #
drop_gift="drop table if exists bigdata.bili_payer_gift_info_tmp;"
drop_guard="drop table if exists bigdata.bili_live_guard_list_value_tmp;"
drop_stat="drop table if exists bigdata.bili_live_guard_stat_value_tmp;"
drop_audience="drop table if exists bigdata.bili_user_audience_num_tmp;"
drop_bc="drop table if exists bigdata.bili_broadcast_row_final_tmp;"

echo -n "删除临时表, BEGIN..."
executeHiveCommand "${drop_gift} ${drop_guard} ${drop_stat} ${drop_audience} ${drop_bc}"
# /usr/bin/hive -e "${drop_gift} ${drop_guard} ${drop_stat} ${drop_audience} ${drop_bc}"
echo -n "删除临时表, END."
