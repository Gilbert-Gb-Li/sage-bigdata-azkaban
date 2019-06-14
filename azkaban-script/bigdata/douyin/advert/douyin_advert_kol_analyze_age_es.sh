#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_calc_data.sh,douyin_advert_content_calc_data.sh
# 导出到ES 粉丝分析 年龄 age

yesterday=$1

dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

cycle=30
    echo "++++++++++++++++++++++++++++++++导出KOL粉丝分析 年龄到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
            insert into bigdata.advert_douyin_user_analyze_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
            SELECT '${stat_date}' AS stat_month,
       unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd') * 1000,
       t5.kol_id,
       'douyin' platform,
       '0' platform_kol_id,
       t8.cert_1,
       'age',
       t6.content_lab,
       count(1) val,
       IF(t8.cert_1 IS NULL,
          0,
          avg(count(1)) over(PARTITION BY t8.cert_1, t6.content_lab)),
       '${dayBeforeYesterday}',
       ${cycle}
  FROM (SELECT kol_id,fans_id user_id, age
          FROM bigdata.douyin_advert_fans_data_snapshot
         WHERE dt = '${dayBeforeYesterday}'
           AND age IS NOT NULL
           AND age != '') t5
  LEFT JOIN (select kol_id, interest_id, cert_label_id
               from bigdata.advert_douyin_kol_mark_daily_snapshot
              where dt = '${yesterday}') t7
    ON t7.kol_id = t5.kol_id
  LEFT JOIN bigdata.advert_cert t8
    ON t7.cert_label_id = t8.id
 INNER JOIN (SELECT content_lab,
                    COALESCE(split(content_val, '-') [ 0 ], 0) minage,
                    COALESCE(split(content_val, '-') [ 1 ], 500) maxage
               FROM bigdata.advert_dictionary
              WHERE category_code = '103'
                AND is_valid = 'Y') t6
 WHERE t5.age >= t6.minage
   AND t5.age <= maxage
 GROUP BY t5.kol_id, t8.cert_1, t6.content_lab;
"
    executeHiveCommand "${COMMON_VAR}${es_sql}"
