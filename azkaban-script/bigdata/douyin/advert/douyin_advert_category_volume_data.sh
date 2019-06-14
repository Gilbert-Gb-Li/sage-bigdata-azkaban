#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖 bigdata.douyin_advert_pbrand_brand_data
# 生成品类声量指数

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 1 7 30 60
do
    echo "++++++++++++++++++++++++++++++++生成品类声量指数 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    search_type=1
    hive_sql="${hive_sql}WITH t_orgin AS
 (SELECT t6.category_id category, t6.category_depth, t4.content_id
    FROM (SELECT t1.pl_1, t1.content_id, split(t3.path, ',') categorys
            FROM (SELECT category_id, content_id, pl_1
                    FROM bigdata.douyin_advert_source_keywords
                   WHERE dt <= '${dayBeforeYesterday}'
                     AND dt > '${date_reduce}'
                     AND to_date(source_date) > '${date_reduce}'
                   GROUP BY pl_1, category_id, content_id) t1
           INNER JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t3
              ON t1.category_id = t3.id) t4,
         bigdata.douyin_advert_pbrand_brand_data t6
   WHERE array_contains(t4.categorys, t6.category_id))
insert overwrite table bigdata.douyin_advert_category_volume partition
 (dt = '${dayBeforeYesterday}', cycle = ${cycle})
select t1.category,
       t1.category_depth,
       min(if(count(1) = 0, 0, round(sum(interact_num) / count(1)))) over(partition by t1.category_depth) min_interact,
       max(if(count(1) = 0, 0, round(sum(interact_num) / count(1)))) over(partition by t1.category_depth) max_interact,
       if(count(1) = 0, 0, round(sum(interact_num) / count(1))) brand_interact,
       round(1 +
             round((100 - 1) / (max(if(count(1) = 0,
                                       0,
                                       round(sum(interact_num) / count(1))))
                    over(partition by t1.category_depth) -
                    min(if(count(1) = 0,
                                       0,
                                       round(sum(interact_num) / count(1))))
                    over(partition by t1.category_depth)),
                   10) *
             (if(count(1) = 0, 0, round(sum(interact_num) / count(1))) -
             min(if(count(1) = 0, 0, round(sum(interact_num) / count(1))))
              over(partition by t1.category_depth)),
             10) volume
  from t_orgin t1
 inner join (select content_id, interact_num
               from bigdata.douyin_advert_content_calc_data
              where dt = '${dayBeforeYesterday}'
                and cycle = ${cycle}) t2
    on t1.content_id = t2.content_id
 group by t1.category_depth, t1.category;
"
done
    executeHiveCommand "${COMMON_VAR}${hive_sql}"

