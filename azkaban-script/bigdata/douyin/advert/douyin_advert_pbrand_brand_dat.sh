#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_source_keywords 分词表数据
# 生成 品类，父品牌和子品牌对应关系

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql="insert overwrite table bigdata.douyin_advert_pbrand_brand_data
SELECT category_id, p_brand_id, brand_id, brand_name, brand_depth, category_depth, p_category_id
FROM ( SELECT t.category category_id, t5.pid p_brand_id, t.brand brand_id, t5. NAME brand_name,
              t5.depth brand_depth, t6.depth category_depth, t6.pid p_category_id
       FROM ( SELECT brand, category
              FROM ( SELECT split (t2.path, ',') brands, split (t3.path, ',') categorys
                     FROM ( SELECT brand_id, category_id
                            FROM bigdata.douyin_advert_source_keywords
                            WHERE dt >= '${dayBeforeYesterday}'
                            AND to_date(source_date) = '${dayBeforeYesterday}'
                            GROUP BY brand_id, category_id ) t1
                     INNER JOIN (select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') t2
                     ON t1.brand_id = t2.id
                     INNER JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t3
                     ON t1.category_id = t3.id ) t4
             LATERAL VIEW explode (t4.brands) v_1 AS brand
             LATERAL VIEW explode (t4.categorys) v_2 AS category
             GROUP BY brand, category ) t
       INNER JOIN (select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') t5
       ON t.brand = t5.id
       INNER JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t6
       ON t.category = t6.id
       UNION ALL
       SELECT category_id, p_brand_id, brand_id, brand_name, brand_depth, category_depth, p_category_id
       FROM bigdata.douyin_advert_pbrand_brand_data ) t
GROUP BY category_id, p_brand_id, brand_id, brand_name,brand_depth, category_depth, p_category_id;"

echo "++++++++++++++++++++++++++++++++生成品类、父品牌和子品牌对应关系中间表++++++++++++++++++++++++++++++++++++++"
executeHiveCommand "${COMMON_VAR}${hive_sql}"