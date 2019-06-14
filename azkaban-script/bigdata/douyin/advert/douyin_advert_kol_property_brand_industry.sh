#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_brand_keywords 中间表先生成
# 生成kol和行业数组和品牌数组对应关系
# 与统计周期有关

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 7 30 60
do
    echo "++++++++++++++++++++++++++++++++计算 KOL涉及的行业和品牌 统计周期'${cycle}' 中间表++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    hive_sql1="INSERT overwrite table bigdata.douyin_advert_kol_property_brand_industry PARTITION (dt = '${dayBeforeYesterday}',cycle=${cycle})
select t.kol_id,p.industrys,b.brands
from (select user_id kol_id
      from bigdata.douyin_advert_kol_snapshot
      where dt='${dayBeforeYesterday}') t
left join
     (select kol_id,collect_set(industry) industrys
      from (select kol_id,industry
            from (select p1.kol_id,p2.path
                  from bigdata.douyin_advert_source_keywords p1,bigdata.advert_category p2
                  where p1.category_id=p2.id and p1.dt>'${date_reduce}' and p1.dt<='${dayBeforeYesterday}' and to_date(p1.source_date)>'${date_reduce}') p3
            LATERAL VIEW explode(split(p3.path,',')) path_arr1 as industry) p4
      group by kol_id) p
on t.kol_id=p.kol_id
left join
      (select kol_id,collect_set(brand) brands
       from (select kol_id,brand
             from (select b1.kol_id,b2.path
                   from bigdata.douyin_advert_source_keywords b1,(select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') b2
                   where b1.brand_id=b2.id and b1.dt>'${date_reduce}' and b1.dt<='${dayBeforeYesterday}' and to_date(b1.source_date)>'${date_reduce}') b3
             LATERAL VIEW explode(split(b3.path,',')) path_arr2 as brand) b4
       group by kol_id) b
on t.kol_id=b.kol_id
where p.industrys is not null or b.brands is not null;"
    executeHiveCommand "${COMMON_VAR}${hive_sql1}"
done
