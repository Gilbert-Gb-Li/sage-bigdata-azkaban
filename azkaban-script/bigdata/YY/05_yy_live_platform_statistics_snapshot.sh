#!/bin/sh
source /etc/profile
#source /home/hadoop/yy/env.conf
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
last_week=`date -d "-7 day $date" +%Y-%m-%d`
last_month=`date -d "-30 day $date" +%Y-%m-%d`

add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_commen="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

# ----------------------------------------------
# -- 观众2 自然日 平台
# ----------------------------------------------
t_online_num="create temporary table bigdata.yy_tmp_online_num as
    select max(online_num) online_num
    from(
        select data_generate_time,sum(online_num) online_num
        from bigdata.yy_live_id_list_data_origin
        where dt='${date}'
        group by data_generate_time
    ) a;"

#--------------------------------------------------
#- live audience user temporary 互动人数
#--------------------------------------------------
t_audience_num="create temporary table bigdata.yy_tmp_audience_num as
    select count(1) as audience_num
    from(
        select user_id,audience_id
        from bigdata.YY_live_danmu_data_origin
        where dt='${date}'
        group by user_id,audience_id
    ) t;"

#------------------------------------------------
#- 开播数 平台 自然日
#------------------------------------------------
t_bc_num="create temporary table bigdata.yy_tmp_bc_num as
        select count(*) bc_num
        from (
            select user_id,start_time
            from bigdata.YY_live_id_list_data_origin
            where dt='${date}'
            group by user_id,start_time
        ) a;"

#---------------------------------------------
#-- 活跃主播数
#---------------------------------------------
t_active_num="create temporary table bigdata.yy_tmp_user_active_num as
    select
    x.num1 + y.num2 as user_active_num
    from
        (select count(1) num1
        from bigdata.yy_live_id_list_all_snapshot
        where dt='${date}' and update_date='${date}') x,
        (select count(1) num2
        from (
            select a.audience_id
            from
                (select audience_id
                from bigdata.yy_live_payer_info_all_snapshot
                where dt='${date}' and update_date='${date}') a
            join
                (select user_id
                from bigdata.yy_live_id_list_all_snapshot
                where dt='${date}') b
            on a.audience_id = b.user_id
        ) c) y;"

t_7_active_num="create temporary table bigdata.yy_tmp_user_active_num_7 as
    select
    x.num1 + y.num2 as user_active_num
    from
        (select count(1) num1
        from bigdata.yy_live_id_list_all_snapshot
        where dt='${date}' and update_date>='${last_week}') x,
        (select count(1) num2
        from (
            select a.audience_id
            from
                (select audience_id
                from bigdata.yy_live_payer_info_all_snapshot
                where dt='${date}' and update_date='${last_week}') a
            join
                (select user_id
                from bigdata.yy_live_id_list_all_snapshot
                where dt='${date}') b
            on a.audience_id = b.user_id
        ) c) y;"

t_30_active_num="create temporary table bigdata.yy_tmp_user_active_num_30 as
    select
    x.num1 + y.num2 as user_active_num
    from
        (select count(1) num1
        from bigdata.yy_live_id_list_all_snapshot
        where dt='${date}' and update_date>='${last_month}') x,
        (select count(1) num2
        from (
            select a.audience_id
            from
                (select audience_id
                from bigdata.yy_live_payer_info_all_snapshot
                where dt='${date}' and update_date>='${last_month}') a
            join
                (select user_id
                from bigdata.yy_live_id_list_all_snapshot
                where dt='${date}') b
            on a.audience_id = b.user_id
        ) c) y;"


#------------------------------------------
#-- 合并结果
#-----------------------------------------
statistics="insert into table bigdata.yy_es_live_platform_statistics_snapshot
select concat('yy_platform_statistics-',dt) es_id,
    a.online_num,b.audience_num,c.bc_num,
    d.user_active_num today_active_num,
    e.user_active_num week_active_num,
    f.user_active_num month_active_num,
    'yy_platform_statistics' as meta_table_name,
    'com.duowan.mobile' as meta_app_name,
    g.dt,substr(g.dt,0,7) months
from
    bigdata.yy_tmp_online_num a,
    bigdata.yy_tmp_audience_num b,
    bigdata.yy_tmp_bc_num c,
    bigdata.yy_tmp_user_active_num d,
    bigdata.yy_tmp_user_active_num_7 e,
    bigdata.yy_tmp_user_active_num_30 f,
    (select '$date' dt) g;"


echo "计算统计结果并写入ES..., date: ${date}"
executeHiveCommand "${t_online_num} ${t_audience_num} ${t_bc_num}
    ${t_active_num} ${t_7_active_num} ${t_30_active_num}
    ${add_es_hadoop} ${add_commen} ${statistics}"
echo "操作完成,OK"


#------------------------------------------------
#-- 工会数 平台 自然日
#-- ES 聚合实现
#------------------------------------------------
#select count(*) family_num
#from(
#    select user_family
#    from bigdata.yy_live_user_info_all_snapshot
#    where dt='2019-01-15' and update_date='2019-01-15'
#    group by user_family
#) a

#------------------------------------------------
#-- 商铺数 平台 自然日
#-- ES 聚合实现
#------------------------------------------------
#select count(user_id) shops_num
#from bigdata.yy_live_user_info_all_snapshot
#where dt='2019-01-15' and update_date='2019-01-15' and business is not null

#------------------------------------------------
#-- 累计平台主播数
#-- ES 聚合实现
#------------------------------------------------
#create temporary table bigdata.yy_user_num_tmp as
#select count(user_id) user_num
#from bigdata.yy_live_user_info_all_snapshot
#where dt='2019-01-15'

#------------------------------------------------
#- 打赏用户数 平台 自然日
#------------------------------------------------
#create temporary table bigdata.yy_gift_num_tmp as
#    select count(*) gift_num
#    from (
#        select audience_id
#        from bigdata.yy_live_payer_info_all_snapshot
#        where dt='2019-01-15' and pay_type=1
#        group by audience_id
#    ) a


#----------------------------------------------
#- 打赏礼物流水 平台 自然日
#- ES 聚合实现
#----------------------------------------------
#create temporary table bigdata.yy_gift_val_tmp as
#    select sum(gift_val) gift_val
#    from bigdata.yy_live_payer_info_daily_snapshot
#    where dt= '2019-01-15' and pay_type=1
#

#---------------------------------------------
#-- 删除临时表
#---------------------------------------------
drop_audience="drop table if exists bigdata.yy_user_audience_num_tmp;"
drop_gift="drop table if exists bigdata.yy_gift_info_all_tmp;"
drop_bc="drop table if exists bigdata.YY_live_broadcast_info_tmp;"

echo "删除临时表..."
executeHiveCommand "${drop_audience} ${drop_gift} ${drop_bc}"
echo "删除完成,OK"
