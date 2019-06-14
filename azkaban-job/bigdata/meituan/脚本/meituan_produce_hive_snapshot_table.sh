#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
date=$1
yesterday=`date -d "-0 day $date" +%Y-%m-%d`
preyesterday=`date -d "-1 day $date" +%Y-%m-%d`
weekday=`date -d "-6 day $date" +%Y-%m-%d`
month2day=`date -d "-29 day $date" +%Y-%m-%d`
echo ${yesterday}
echo ${preyesterday}
echo ${weekday}
echo ${month2day}
echo "=================================生成每天去重后的商户表==================================="
sql1="insert into table bigdata.meituan_financial_shop_list_snapshot PARTITION(dt='${yesterday}')
    select
     b.restaurant_id ,
     b.shop_name ,
     b.shop_id ,
     b.address ,
     b.shop_support_self_taking ,
     b.phone ,
     b.categories,
     b.open_hours ,
     b.shop_logo ,
     b.shop_rating ,
     b.shop_recent_food_popularity,
     b.shop_min_order_amount ,
	 b.shop_average_price,
     b.shop_delivery_mode ,
	 b.shop_label_icon,
     b.shop_delivery_price ,
     b.latitude ,
     b.longitude ,
     b.ias_timestamp,
     split(b.current_page,'_')[0],
	 split(b.current_page,'_')[1],
	 split(b.current_page,'_')[2],
     b.meta_name
     from
     (select
     a.restaurant_id ,
     a.shop_name ,
     a.shop_id ,
     a.address ,
     a.shop_support_self_taking ,
     a.phone ,
     a.categories,
     a.open_hours ,
     a.shop_logo ,
     a.shop_rating ,
     a.shop_recent_food_popularity,
     a.shop_min_order_amount ,
     a.shop_average_price,
     a.shop_delivery_mode ,
	 a.shop_label_icon,
     a.shop_delivery_price ,
     a.latitude ,
     a.longitude ,
     a.ias_timestamp,
     a.current_page,
     a.meta_name,
     row_number() over (partition by a.shop_id order by a.ias_timestamp desc) as order_num
 from  bigdata.meituan_financial_shop_list_origin a
where a.dt = '${yesterday}' and a.shop_id is not null and  a.latitude is not null and a.longitude is not null) b
where b.order_num=1;"
echo "=================================生成每天新增的商户表==================================="
sql2="insert into table bigdata.meituan_financial_new_shop_list_snapshot PARTITION(dt='${yesterday}')
        select
          c1.restaurant_id ,
          c1.shop_name ,
          c1.shop_id ,
          c1.address ,
          c1.shop_support_self_taking ,
          c1.phone ,
          c1.categories,
          c1.open_hours ,
          c1.shop_logo ,
          c1.shop_rating ,
          c1.shop_recent_food_popularity,
          c1.shop_min_order_amount ,
          c1.shop_average_price,
		  c1.shop_delivery_mode ,
		  c1.shop_label_icon,
          c1.shop_delivery_price ,
          c1.latitude ,
          c1.longitude ,
          c1.ias_timestamp,
          c1.current_page,
		  c1.province,
		  c1.city,
          c1.meta_name
        from
        (select * from bigdata.meituan_financial_shop_list_snapshot where dt='${yesterday}'
        ) as c1
        left join (
        select shop_id from bigdata.meituan_financial_all_shop_list_snapshot where dt='${preyesterday}'
        ) as c2
        on c1.shop_id = c2.shop_id
        where  c2.shop_id is null;"
echo "=================================生成每天全量商户表==================================="
sql3="insert into table bigdata.meituan_financial_all_shop_list_snapshot PARTITION(dt='${yesterday}')
          select
              restaurant_id ,
              shop_name ,
              shop_id ,
              address ,
              shop_support_self_taking ,
              phone ,
              categories,
              open_hours ,
              shop_logo ,
              shop_rating ,
              shop_recent_food_popularity,
              shop_min_order_amount ,
              shop_average_price,
			  shop_delivery_mode ,
			  shop_label_icon,
              shop_delivery_price ,
              latitude ,
              longitude ,
              ias_timestamp,
              current_page,
			  province,
			  city,
              meta_name
            from
            bigdata.meituan_financial_new_shop_list_snapshot where dt='${yesterday}'
			union all
			select
              restaurant_id ,
              shop_name ,
              shop_id ,
              address ,
              shop_support_self_taking ,
              phone ,
              categories,
              open_hours ,
              shop_logo ,
              shop_rating ,
              shop_recent_food_popularity,
              shop_min_order_amount ,
              shop_average_price,
			  shop_delivery_mode ,
			  shop_label_icon,
              shop_delivery_price ,
              latitude ,
              longitude ,
              ias_timestamp,
              current_page,
			  province,
			  city,
              meta_name
            from
            bigdata.meituan_financial_all_shop_list_snapshot where dt='${preyesterday}';"

#echo "=================================生成每日爬取商家表按品类拆分后的表==================================="
#sql6="insert into table bigdata.meituan_financel_chai_category_snapshot PARTITION(dt='${yesterday}')
#      select b.jingweiming1, category2,
#      b.occur_timestamp1, b.meta_name1 from(
#      select a.jingweiming jingweiming1, a.occur_timestamp occur_timestamp1,
#       a.meta_name meta_name1 ,split(a.categories,',') category1
#      from bigdata.ele_financial_shop_list_snapshot a
#      where a.dt='${yesterday}') b lateral view explode(
#      b.category1) adtable as category2;"

echo "==============================从原始表生成商家菜品列表中间表==================================="
sql4="insert into table bigdata.meituan_financial_shop_menu_list_snapshot PARTITION(dt='${yesterday}')
       select
	   CONCAT_WS(\"_\",b.shop_id,b.menu_id,b.menu_quantity) as unique_id,
	   b.shop_id,
       b.menu_name ,
       b.menu_id  ,
       b.menu_origin_price  ,
       b.menu_price   ,
	   b.menu_quantity  ,
       b.menu_packing_fee  ,
	   b.menu_packing_num  ,
       b.menu_is_off_sell  ,
       b.menu_month_sales  ,
       b.menu_rating   ,
       b.menu_rating_count   ,
       b.menu_satisfy_rate   ,
       b.menu_food_spec   ,
       b.menu_max_promotion_quantity_detail_text,
       b.menu_promotion_is_Must_super_vip,
       b.restaurant_id   ,
       b.ias_timestamp   ,
       b.meta_name
       from 
	   (select
      a.shop_id,
       a.menu_name ,
       a.menu_id  ,
       a.menu_origin_price  ,
       a.menu_price   ,
	   a.menu_quantity  ,
       a.menu_packing_fee  ,
	   a.menu_packing_num  ,
       a.menu_is_off_sell  ,
       a.menu_month_sales  ,
       a.menu_rating   ,
       a.menu_rating_count   ,
       a.menu_satisfy_rate   ,
       a.menu_food_spec   ,
       a.menu_max_promotion_quantity_detail_text,
       a.menu_promotion_is_Must_super_vip,
       a.restaurant_id   ,
       a.ias_timestamp   ,
       a.meta_name,
     row_number() over (partition by a.shop_id,a.menu_id,a.menu_quantity order by a.ias_timestamp desc) as order_num
 from  bigdata.meituan_financial_shop_menu_list_origin a
where a.dt = '${yesterday}' and a.shop_id is not null ) b
where b.order_num=1;"
	   
echo "==============================生成商家新增菜品列表中间表==================================="
sql5="insert into table bigdata.meituan_financial_new_shop_menu_list_snapshot PARTITION(dt='${yesterday}')
	select
       c1.unique_id,
	   c1.shop_id,
       c1.menu_name ,
       c1.menu_id  ,
       c1.menu_origin_price  ,
       c1.menu_price   ,
	   c1.menu_quantity  ,
       c1.menu_packing_fee  ,
	   c1.menu_packing_num  ,
       c1.menu_is_off_sell  ,
       c1.menu_month_sales  ,
       c1.menu_rating   ,
       c1.menu_rating_count   ,
       c1.menu_satisfy_rate   ,
       c1.menu_food_spec   ,
       c1.menu_max_promotion_quantity_detail_text,
       c1.menu_promotion_is_Must_super_vip,
       c1.restaurant_id   ,
       c1.ias_timestamp   ,
       c1.meta_name
        from
        (select * from bigdata.meituan_financial_shop_menu_list_snapshot where dt='${yesterday}'
        ) as c1
        left join (
        select unique_id from bigdata.meituan_financial_all_shop_menu_list_snapshot where dt='${preyesterday}'
        ) as c2
        on c1.unique_id = c2.unique_id
        where  c2.unique_id is null;"
echo "==============================生成全量商家菜品列表中间表==================================="
sql6="insert into table bigdata.meituan_financial_all_shop_menu_list_snapshot PARTITION(dt='${yesterday}')
	select
              unique_id,
	   shop_id,
       menu_name ,
       menu_id  ,
       menu_origin_price  ,
       menu_price   ,
	   menu_quantity  ,
       menu_packing_fee  ,
	   menu_packing_num  ,
       menu_is_off_sell  ,
       menu_month_sales  ,
       menu_rating   ,
       menu_rating_count   ,
       menu_satisfy_rate   ,
       menu_food_spec   ,
       menu_max_promotion_quantity_detail_text,
       menu_promotion_is_Must_super_vip,
       restaurant_id   ,
       ias_timestamp   ,
       meta_name
            from
            bigdata.meituan_financial_new_shop_menu_list_snapshot where dt='${yesterday}'
			union all
			select
              unique_id,
	   shop_id,
       menu_name ,
       menu_id  ,
       menu_origin_price  ,
       menu_price   ,
	   menu_quantity  ,
       menu_packing_fee  ,
	   menu_packing_num  ,
       menu_is_off_sell  ,
       menu_month_sales  ,
       menu_rating   ,
       menu_rating_count   ,
       menu_satisfy_rate   ,
       menu_food_spec   ,
       menu_max_promotion_quantity_detail_text,
       menu_promotion_is_Must_super_vip,
       restaurant_id   ,
       ias_timestamp   ,
       meta_name
            from
            bigdata.meituan_financial_all_shop_menu_list_snapshot where dt='${preyesterday}';"

   echo "================================生成每天去重后的活跃商户表==================================="
    executeHiveCommand "${sql1}"
   echo "=================================生成每天新增的商户表==================================="
    executeHiveCommand "${sql2}"
   echo "=================================生成每天全量商户表=========================================="
    executeHiveCommand "${sql3}"
   echo "==============================生成活跃商家菜品列表中间表==================================="
    executeHiveCommand "${sql4}"
   echo "==============================生成商家新增菜品列表中间表==================================="
	executeHiveCommand "${sql5}"
   echo "==============================生成全量商家菜品列表中间表==================================="
	executeHiveCommand "${sql6}"