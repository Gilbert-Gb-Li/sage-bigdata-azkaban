#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖 bigdata.douyin_advert_pbrand_brand_data
# 生成品牌声量指数

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 1 7 30 60
do
    echo "++++++++++++++++++++++++++++++++生成品牌声量指数 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    search_type=1
    hive_sql="${hive_sql}with t_orgin as
 (select t6.brand_id brand, t6.brand_depth, content_id
    from (select t1.content_id,
                 t1.kol_id,
                 split(t2.path, ',') brands,
                 split(t3.path, ',') categorys
            from (select category_id, brand_id, content_id, kol_id
                    from bigdata.douyin_advert_source_keywords
                   where dt <= '${dayBeforeYesterday}'
                     and dt > '${date_reduce}'
                     and to_date(source_date) > '${date_reduce}') t1
           inner join (select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') t2
              on t1.brand_id = t2.id
           left join (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t3
              on t1.category_id = t3.id) t4,
         bigdata.douyin_advert_pbrand_brand_data t6
   where array_contains(t4.brands, t6.brand_id)
     and array_contains(t4.categorys, t6.category_id)
   group by t6.brand_id, t6.brand_depth, content_id)
insert overwrite table bigdata.douyin_advert_brand_volume partition
 (dt = '${dayBeforeYesterday}', cycle = ${cycle})
select t1.brand,
       t1.brand_depth,
       min(if(count(1) = 0, 0, round(sum(interact_num) / count(1)))) over(partition by t1.brand_depth) min_interact,
       max(if(count(1) = 0, 0, round(sum(interact_num) / count(1)))) over(partition by t1.brand_depth) max_interact,
       if(count(1) = 0, 0, round(sum(interact_num) / count(1))) brand_interact,
       round(1 +
             round((100 - 1) / (max(if(count(1) = 0,
                                       0,
                                       round(sum(interact_num) / count(1))))
                    over(partition by t1.brand_depth) -
                    min(if(count(1) = 0,
                                       0,
                                       round(sum(interact_num) / count(1))))
                    over(partition by t1.brand_depth)),
                   10) *
             (if(count(1) = 0, 0, round(sum(interact_num) / count(1))) -
             min(if(count(1) = 0, 0, round(sum(interact_num) / count(1))))
              over(partition by t1.brand_depth)),
             10) volume
  from t_orgin t1
 inner join (select content_id, interact_num
               from bigdata.douyin_advert_content_calc_data
              where dt = '${dayBeforeYesterday}'
                and cycle = ${cycle}) t2
    on t1.content_id = t2.content_id
 group by t1.brand_depth, t1.brand;
"
done
    executeHiveCommand "${COMMON_VAR}${hive_sql}"
