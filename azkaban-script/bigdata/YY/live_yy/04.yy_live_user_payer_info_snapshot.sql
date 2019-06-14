use bigdata;

--------------------------
-- 付费用户日统计表，日明细表
-- 单单使用累计表会删除当天的付费明细
--------------------------
insert into bigdata.YY_live_payer_info_daily_snapshot partition (dt='2019-01-15')
    select
        audience_id,user_id,sum(gift_val) gift_val,pay_type,sum(gift_num) gift_num
    from
        bigdata.yy_gift_info_all_tmp
    group by audience_id,user_id,pay_type;



----------------------------
-- 累计付费用户数据，全量按天分区
----------------------------
insert into table bigdata.yy_live_payer_info_all_snapshot partition (dt='2019-01-25')
select update_date,audience_id,audience_name,pay_type,data_generate_time
from
(select
  *,row_number () over (partition by audience_id order by data_generate_time desc) rowid
 from(
   select dt update_date,audience_id,audience_name,1 as pay_type,data_generate_time
   from bigdata.YY_live_danmu_data_origin
   where
   gift_num > 0 and dt='2019-01-25'
   union all
    select update_date,audience_id,audience_name,pay_type,data_generate_time
    from bigdata.yy_live_payer_info_all_snapshot
    where dt='2019-01-24'
 ) a
) t
 where t.rowid = 1;


 -------------------------------
 -- 日付费用户数据同步到ES
 ------------------------------
 insert into table bigdata.yy_es_live_payer_info_daily_snapshot
 select
    audience_id,user_id,gift_val,pay_type,
    'payer_info_daily' as meta_table_name,
    'com.duowan.mobile' as meta_app_name,
    dt
 from bigdata.yy_live_payer_info_daily_snapshot
 where dt='2019-01-15'

-----------------------------------------------
-- 累计付费用户同步到ES
----------------------------------------------
insert into table bigdata.yy_es_live_payer_info_all_snapshot
select update_date,audience_id,audience_name,pay_type,
       'payer_info_all' as meta_table_name,
       'com.duowan.mobile' as meta_app_name,
       dt
from  bigdata.yy_live_payer_info_all_snapshot
where dt='2019-01-15'