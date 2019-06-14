------------------------------------------------
-- 累计礼物列表，插入分区表 每天
------------------------------------------------
insert into table bigdata.yy_live_gift_info_all_snapshot partition (dt='2019-01-16')
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
            dt = '2019-01-30'
        union all
        select
            update_date,gift_id,gift_gold,gift_name,gift_image,data_generate_time,meta_table_name,meta_app_name
        from
            bigdata.YY_live_gift_info_all_snapshot
        where dt='2019-01-29'
            ) as p
     ) as t
where t.order_num = 1;


-----------------------
-- 同步ES
----------------------
insert into table bigdata.yy_es_live_gift_info_all_snapshot
select
    update_date,gift_id,gift_gold,gift_name,gift_image,
    data_generate_time,meta_table_name,meta_app_name,dt
from
    bigdata.yy_live_gift_info_all_snapshot
where
    dt='2019-01-16'


