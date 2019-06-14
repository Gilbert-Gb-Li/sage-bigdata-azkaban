----------------------------------------------
-- 观众2 自然日 平台
----------------------------------------------
create temporary table bigdata.yy_tmp_online_num as
select max(online_num) online_num
from(
    select data_generate_time,sum(online_num) online_num
    from bigdata.YY_live_user_info_data_origin
    where dt='2019-01-15'
    group by data_generate_time
) a

---------------------------------------------------
-- live audience user temporary 互动人数
---------------------------------------------------
create temporary table bigdata.yy_tmp_audience_num AS
select count(1) as audience_num
from(
    select user_id,audience_id
    from bigdata.YY_live_danmu_data_origin
    where dt='2019-01-15'
    group by user_id,audience_id
) t


-------------------------------------------------
-- 开播数 平台 自然日
-------------------------------------------------
create temporary table bigdata.yy_tmp_bc_num as
    select count(*) bc_num
    from (
        select user_id,start_time
        from bigdata.YY_live_id_list_data_origin
        where dt='2019-01-15'
        group by user_id,start_time
    ) a;



---------------------------------------------
-- 活跃主播数
#---------------------------------------------
create temporary table bigdata.yy_tmp_user_active_num as
    select
    x.num1 +y.num2 as user_active_num
    from
        (select count(1) num1
        from bigdata.yy_live_user_info_all_snapshot
        where dt='2019-01-15' and update_date='2019-01-15') x,
        (select count(1) num2
        from (
            select a.audience_id
            from
                (select audience_id
                from bigdata.yy_live_payer_info_all_snapshot
                where dt='2019-01-15' and update_date='2019-01-15') a
            join
                (select user_id
                from bigdata.yy_live_user_info_all_snapshot
                where dt='2019-01-15') b
            on a.audience_id = b.user_id
        ) c) y

create temporary table bigdata.yy_tmp_user_active_num_7 as
    select
    x.num1 +y.num2 as user_active_num
    from
        (select count(1) num1
        from bigdata.yy_live_user_info_all_snapshot
        where dt='2019-01-15' and update_date>='2019-01-13') x,
        (select count(1) num2
        from (
            select a.audience_id
            from
                (select audience_id
                from bigdata.yy_live_payer_info_all_snapshot
                where dt='2019-01-15' and update_date='2019-01-13') a
            join
                (select user_id
                from bigdata.yy_live_user_info_all_snapshot
                where dt='2019-01-15') b
            on a.audience_id = b.user_id
        ) c) y

create temporary table bigdata.yy_tmp_user_active_num_30 as
    select
    x.num1 + y.num2 as user_active_num
    from
        (select count(1) num1
        from bigdata.yy_live_user_info_all_snapshot
        where dt='${date}' and update_date>='2019-01-10') x,
        (select count(1) num2
        from (
            select a.audience_id
            from
                (select audience_id
                from bigdata.yy_live_payer_info_all_snapshot
                where dt='2019-01-15' and update_date>='2019-01-10') a
            join
                (select user_id
                from bigdata.yy_live_user_info_all_snapshot
                where dt='2019-01-15') b
            on a.audience_id = b.user_id
        ) c) y


------------------------------------------
-- 合并结果
-----------------------------------------
insert into table bigdata.yy_es_live_platform_statistics_snapshot
select
    a.online_num,b.audience_num,c.bc_num,
    d.user_active_num today_active_num,
    e.user_active_num week_active_num,
    f.user_active_num month_active_num,
    'yy_platform_statistics' as meta_table_name,
    'com.duowan.mobile' as meta_app_name,
    '2019-01-15' as dt
from
    bigdata.yy_tmp_online_num a,
    bigdata.yy_tmp_audience_num b,
    bigdata.yy_tmp_bc_num c,
    bigdata.yy_tmp_user_active_num d,
    bigdata.yy_tmp_user_active_num_7 e,
    bigdata.yy_user_tmp_active_num_30 f


-------------------------------------------------
-- 打赏用户数
-- ES 聚合实现
-------------------------------------------------
create temporary table bigdata.yy_gift_num_tmp as
select count(*) gift_num
from (
    select audience_id
    from bigdata.yy_live_payer_info_all_snapshot
    where dt='2019-01-15' and pay_type=1
    group by audience_id
) a


-----------------------------------------------
-- 打赏礼物流水 平台 自然日
-- ES 聚合实现
-----------------------------------------------
create temporary table bigdata.yy_gift_val_tmp as
select sum(gift_val) gift_val
from bigdata.yy_live_payer_info_daily_snapshot
where dt= '2019-01-15' and pay_type=1


------------------------------------------------
-- 工会数 平台 自然日
-- ES 聚合实现
------------------------------------------------
select count(*) family_num
from(
    select user_family
    from bigdata.yy_live_user_info_all_snapshot
    where dt='2019-01-15' and update_date='2019-01-15'
    group by user_family
) a

------------------------------------------------
-- 商铺数 平台 自然日
-- ES 聚合实现
------------------------------------------------
select count(user_id) shops_num
from bigdata.yy_live_user_info_all_snapshot
where dt='2019-01-15' and update_date='2019-01-15' and business is not null


------------------------------------------------
-- 累计平台主播数 自然日
-- ES 聚合实现
------------------------------------------------
create temporary table bigdata.yy_user_num_tmp as
select count(user_id) user_num
from bigdata.yy_live_user_info_all_snapshot
where dt='2019-01-15'


---------------------------------------------
-- 删除临时表
---------------------------------------------
drop table if exists bigdata.yy_user_audience_num_tmp;
drop table if exists bigdata.yy_gift_info_all_tmp;
drop table if exists bigdata.YY_live_broadcast_info_tmp;