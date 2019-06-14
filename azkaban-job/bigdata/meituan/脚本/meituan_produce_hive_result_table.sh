#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
date=$1
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
weekday=`date -d "-6 day $date" +%Y-%m-%d`
month2day=`date -d "-29 day $date" +%Y-%m-%d`
echo ${yesterday}
echo ${weekday}
echo ${month2day}

echo "=================================生成每个商户的交易总金额和订单数结果表==================================="
sql1="insert into table bigdata.takeaway_meituan_transactions_shop_result_snapshot partition (dt='${yesterday}')
      select
      a.shop_id,
	  sum(a.menu_month_sales),
      sum(a.menu_month_sales*avg_price)
      from
      (select shop_id,menu_month_sales,avg(menu_price + menu_packing_fee*menu_packing_num) as avg_price from bigdata.meituan_financial_shop_menu_list_snapshot where dt = '${yesterday}' group by shop_id,menu_month_sales,menu_packing_fee,menu_id ) a
      group by a.shop_id;"
	  
echo "=================================生成按takeaway_meituan_result_day_week_month_snapshot商家数日表==================================="
sql2="insert into table bigdata.takeaway_meituan_result_day_week_month_snapshot PARTITION(dt='${yesterday}')
      select
	  b.meituan_quick_totalcount,
      b.meituan_special_totalcount,
      b.self_take_totalcount,
      f.new_shop_count,
      f.new_food_count,
      f.new_not_foot_count,
	  'day' as time_cecle
      from
      (select
      sum (
            case a.shop_delivery_mode
            when '美团快送' then 1 end) as meituan_quick_totalcount,
      sum (
          case a.shop_delivery_mode
          when '美团专送' then 1 end) as meituan_special_totalcount,
      sum (
          case
          when a.shop_delivery_mode!='美团专送' and a.shop_delivery_mode!='美团快送' then 1 end) as self_take_totalcount
      from  bigdata.meituan_financial_shop_list_snapshot a
      where a.dt='${yesterday}') b,
      (select
      count(e.shop_id) as new_shop_count,
      sum (
          case
          when e.current_page='美食' then 1 end) as new_food_count ,
      sum (
        case
        when e.current_page!='美食' then 1 end) as new_not_foot_count
      from
      bigdata.meituan_financial_new_shop_list_snapshot e
      where e.dt='${yesterday}') f;"
	  
echo "=================================生成按takeaway_meituan_result_week_snapshot商家数周表==================================="
sql3="insert into table bigdata.takeaway_meituan_result_day_week_month_snapshot PARTITION(dt='${yesterday}')
      select
	  b.meituan_quick_totalcount,
      b.meituan_special_totalcount,
      b.self_take_totalcount,
      f.new_shop_count,
      f.new_food_count,
      f.new_not_foot_count,
	  'week' as time_cecle
      from
      (select
      sum (
            case a.shop_delivery_mode
            when '美团快送' then 1 end) as meituan_quick_totalcount,
      sum (
          case a.shop_delivery_mode
          when '美团专送' then 1 end) as meituan_special_totalcount,
      sum (
          case
          when a.shop_delivery_mode!='美团专送' and a.shop_delivery_mode!='美团快送' then 1 end) as self_take_totalcount
      from  (select distinct shop_id,shop_delivery_mode from bigdata.meituan_financial_shop_list_snapshot where dt > '${weekday}' )a
      ) b,
      (select
      count(e.shop_id) as new_shop_count,
      sum (
          case
          when e.current_page='美食' then 1 end) as new_food_count ,
      sum (
        case
        when e.current_page!='美食' then 1 end) as new_not_foot_count
      from
      bigdata.meituan_financial_new_shop_list_snapshot e
      where e.dt>'${weekday}') f;"
	  
echo "=================================生成按takeaway_meituan_result_month_snapshot商家数月表==================================="
sql4="insert into table bigdata.takeaway_meituan_result_day_week_month_snapshot PARTITION(dt='${yesterday}')
      select
	  b.meituan_quick_totalcount,
      b.meituan_special_totalcount,
      b.self_take_totalcount,
      f.new_shop_count,
      f.new_food_count,
      f.new_not_foot_count,
	  'month' as time_cecle
      from
      (select
      sum (
            case a.shop_delivery_mode
            when '美团快送' then 1 end) as meituan_quick_totalcount,
      sum (
          case a.shop_delivery_mode
          when '美团专送' then 1 end) as meituan_special_totalcount,
      sum (
          case
          when a.shop_delivery_mode!='美团专送' and a.shop_delivery_mode!='美团快送' then 1 end) as self_take_totalcount
      from  (select distinct shop_id,shop_delivery_mode from bigdata.meituan_financial_shop_list_snapshot where dt > '${month2day}' )a
      ) b,
      (select
      count(e.shop_id) as new_shop_count,
      sum (
          case
          when e.current_page='美食' then 1 end) as new_food_count ,
      sum (
        case
        when e.current_page!='美食' then 1 end) as new_not_foot_count
      from
      bigdata.meituan_financial_new_shop_list_snapshot e
      where e.dt>'${month2day}') f;"
	  
	  
echo "================================= 订单数相关月表 ==================================="
sql5="insert into table bigdata.takeaway_meituan_result_order_platform_snapshot PARTITION(dt='${yesterday}')
	select 
	a.money_total_num,
	e.shop_totalcount,
	c.order_num,
	c.meituan_quick_totalcount,
	c.meituan_special_totalcount,
	c.self_take_totalcount
	from
	(select sum(f.menu_month_sales * f.avg_price) as money_total_num from 
	(select menu_month_sales,menu_packing_fee,avg(menu_price + menu_packing_fee*menu_packing_num) as avg_price from bigdata.meituan_financial_shop_menu_list_snapshot where dt = '${yesterday}' group by menu_month_sales,menu_packing_fee,menu_id ) f ) a,
	(select 
	sum (b.shop_recent_food_popularity) as order_num,
	sum (case b.shop_delivery_mode when '美团快送' then b.shop_recent_food_popularity end) as meituan_quick_totalcount, 
	sum (case b.shop_delivery_mode when '美团专送' then b.shop_recent_food_popularity end) as meituan_special_totalcount,
	sum (case when b.shop_delivery_mode!='美团专送' and b.shop_delivery_mode!='美团快送' then b.shop_recent_food_popularity end) as self_take_totalcount
	 from bigdata.meituan_financial_shop_list_snapshot b) c,
	 (select count(shop_id) as shop_totalcount from bigdata.meituan_financial_all_shop_list_snapshot where dt = '${yesterday}' )e;"

	 
echo "================================= 生成每个商户的交易总金额和订单数结果表 ==========================="
executeHiveCommand "${sql1}"
echo "================================= 生成按takeaway_meituan_result_day_snapshot商家数日表 ==================================="
executeHiveCommand "${sql2}"
echo "================================= 生成按takeaway_meituan_result_week_snapshot商家数周表 ==================================="
executeHiveCommand "${sql3}"
echo "================================= 生成按takeaway_meituan_result_month_snapshot商家数月表 ==================================="
executeHiveCommand "${sql4}"
echo "================================= 订单数相关月表  ==================================="
executeHiveCommand "${sql5}"