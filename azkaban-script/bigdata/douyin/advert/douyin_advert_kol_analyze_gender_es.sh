#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_calc_data.sh,douyin_advert_content_calc_data.sh
# 导出到ES 粉丝分析 性别 gender

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

cycle=30
    echo "++++++++++++++++++++++++++++++++导出KOL粉丝分析 性别到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
            insert into bigdata.advert_douyin_user_analyze_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
            SELECT '${stat_date}' AS stat_month,
       unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd') * 1000,
       t.kol_id,
       'douyin' platform,
       '0' platform_kol_id,
       NULL cert_id,
       'gender',
       CASE t.sex
         WHEN '0' THEN
          '女'
         WHEN '1' THEN
          '男'
         ELSE
          '其它'
       END col,
       count(1) val,
       0,
       '${dayBeforeYesterday}',
       ${cycle}
  FROM (SELECT kol_id,fans_id user_id, sex
          FROM bigdata.douyin_advert_fans_data_snapshot
         WHERE dt = '${dayBeforeYesterday}') t
 GROUP BY t.kol_id, t.sex;
"
    executeHiveCommand "${COMMON_VAR}${es_sql}"
