#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_calc_data 中间表先生成
# 导出到ES KOL排名
# 与统计周期有关

yesterday=$1
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 7 30 60
do
    echo "++++++++++++++++++++++++++++++++生成KOL排名 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    interest_hive_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
               insert into bigdata.advert_douyin_rank_calc_es partition
                 (dt = '${dayBeforeYesterday}', cycle = ${cycle})
                 select '${stat_date}' as stat_month,
                        unix_timestamp( '${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
                        t.kol_id,
                        'douyin' platform,
                        1 type,
                        interestid,
                        i.name col_name,
                        i.depth,
                        t.interact_avg,
                        odr,
                        '${dayBeforeYesterday}',
                        ${cycle}
                   from (select kol_id,
                                interestid,
                                interact_avg,
                                row_number() over(partition by interestid order by interact_avg desc) odr
                           from (select t0.kol_id, t2.path, t0.interact_avg
                                   from bigdata.douyin_advert_kol_calc_data t0,
                                        (select kol_id,interest_id,cert_label_id
                                        from bigdata.advert_douyin_kol_mark_daily_snapshot
                                        where dt='${yesterday}')  t1,
                                        bigdata.advert_interest             t2
                                  where t1.interest_id = t2.id
                                    and t0.kol_id = t1.kol_id
                                    and t0.dt = '${dayBeforeYesterday}'
                                    and t0.cycle = ${cycle}) t4 LATERAL VIEW explode(split(t4.path, ',')) path_arr as interestid) t,
                        bigdata.advert_interest i
                  where t.interestid = i.id;"
    credit_hive_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
                insert into bigdata.advert_douyin_rank_calc_es partition
                  (dt = '${dayBeforeYesterday}', cycle = ${cycle})
                  select '${stat_date}' as stat_month,
                         unix_timestamp( '${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
                         t.kol_id,
                          'douyin' platform,
                         2 type,
                         certid,
                         i.name col_name,
                         i.depth,
                         t.interact_avg,
                         odr,
                         '${dayBeforeYesterday}',
                         ${cycle}
                    from (select kol_id,
                                 certid,
                                 interact_avg,
                                 row_number() over(partition by certid order by interact_avg desc) odr
                            from (select t0.kol_id, t2.path, t0.interact_avg
                                    from bigdata.douyin_advert_kol_calc_data t0,
                                         (select kol_id,interest_id,cert_label_id
                                         from bigdata.advert_douyin_kol_mark_daily_snapshot
                                         where dt='${yesterday}')  t1,
                                         bigdata.advert_cert                 t2
                                   where t1.cert_label_id = t2.id
                                     and t0.kol_id = t1.kol_id
                                     and t0.dt = '${dayBeforeYesterday}'
                                     and t0.cycle = ${cycle}) t4 LATERAL VIEW explode(split(t4.path, ',')) path_arr as certid) t,
                         bigdata.advert_cert i
                   where t.certid = i.id;"
    executeHiveCommand "${COMMON_VAR}${interest_hive_sql}"
    executeHiveCommand "${COMMON_VAR}${credit_hive_sql}"
done