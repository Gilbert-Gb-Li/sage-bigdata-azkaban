use bigdata;

-----------------------------------------------
-- 累计主播列表日临时表
-----------------------------------------------
insert into table bigdata.yy_live_user_info_all_snapshot partition (dt='2019-01-15')
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
            YY_live_user_info_data_origin
        where
            dt = '2019-01-15' and is_live = 1
        union all
        select
            update_date,user_id,user_name,live_desc,user_age,user_sex,user_label_list,
            share_url,location,fans_num,room_id,follow_num,user_family,user_integral,last_start_time,
            user_level,is_live,online_num,contribution_score,sold_in_month_count,classfication,
            business,data_generate_time,meta_table_name,meta_app_name
        from
            yy_live_user_info_all_snapshot
        where dt = '2019-01-15'
        ) as a
    ) as t
where
    t.order_desc = 1

------------------------------------------
-- 日主播数据表
------------------------------------------
insert into table bigdata.yy_live_user_info_daily_snapshot partition (dt='2019-01-15')
select
    a.user_id,a.user_name,a.start_time,a.end_time,a.duration,
    b.audience_num,c.gift_num,c.gift_val,
    'user_info_daily' as meta_table_name,
    'com.duowan.mobile' as meta_app_name
from bigdata.YY_live_broadcast_info_tmp a
left join
    bigdata.yy_user_audience_num_tmp b
on a.user_id = b.user_id
left join
    (select
        user_id,count(audience_id) gift_num, sum(gift_val) gift_val
    from
        bigdata.yy_gift_info_all_tmp
    group by user_id) c
on a.user_id = c.user_id;


-----------------------------------------
-- 累计主播表同步到ES
-----------------------------------------
insert into table bigdata.yy_es_live_user_info_all_snapshot
select
    update_date,user_id,user_name,live_desc,user_age,user_sex,user_label_list,
    share_url,location,fans_num,room_id,follow_num,user_family,user_integral,last_start_time,
    user_level,is_live,online_num,
    contribution_score,sold_in_month_count,classfication,
    business,data_generate_time,meta_table_name,meta_app_name,dt
from bigdata.yy_live_user_info_all_snapshot
where dt='2019-01-16'


----------------------------------
-- 日主播表同步ES
----------------------------------
insert into table bigdata.yy_es_live_user_info_daily_snapshot
select
    user_id,user_name,start_time,end_time,duration,
    audience_num,gift_num,gift_val,
    meta_table_name,meta_app_name,dt
from bigdata.yy_live_user_info_daily_snapshot
where dt='2019-01-15'



