#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_property_brand_industry.sh,douyin_advert_kol_calc_data.sh,douyin_advert_content_calc_data.sh
# 导出到ES KOL统计信息

yesterday=$1
yesterday=`date -d "-1 day $today" +%Y-%m-%d`
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 1 7 30 60
do
    echo "++++++++++++++++++++++++++++++++导出KOL统计信息列表到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
        insert into bigdata.advert_douyin_user_all_es partition(dt='${dayBeforeYesterday}',cycle=${cycle})
        SELECT '${stat_date}' AS stat_month,
               unix_timestamp(t.dt, 'yyyy-MM-dd')*1000,
               t.kol_id,
               'douyin' platform,
               '0' platform_kol_id,
               u.nick_name kol_name,
               bi.pls,
               bi.brands,
               ic.interest_class,
               ic.cert_label,
               u.follower_count,
               u.age,
               u.sex,
               u.province,
               u.city,
               u.avatar_url,
               '${dayBeforeYesterday}' source_time,
               coalesce(t.cover_fans_num,0),
               coalesce(t.interact_fans_num,0),
               coalesce(t.fans_interact_rate,0),
               coalesce(t.content_num,0),
               coalesce(t.content_interact_num,0),
               coalesce(t.interact_avg,0),
               coalesce(t.impact_index,0),
               coalesce(t.impact_incr,0),
               coalesce(d.mention_content_num,0),
               round(d.mention_content_num / t.content_num, 5),
               coalesce(d.mention_content_interact_num,0),
               coalesce(d.mention_content_interact_avg,0),
               coalesce(round((t.content_interact_num - d.mention_content_interact_num) /
                     (t.content_num - d.mention_content_num),
                     5),0),
               coalesce(t.fans_num - t.false_fans_num,0) high_fans_count,
               coalesce(round((t.fans_num - t.false_fans_num) / t.fans_num, 5),0),
               ic.f_cert_name top_cert_name,
               coalesce(round(AVG(t.fans_interact_rate)
                              over(PARTITION BY ic.f_cert_id),
                              5),
                        0) cert_fans_interact_avg,
               coalesce(round(AVG(round(d.mention_content_num / t.content_num, 5))
                              over(PARTITION BY ic.f_cert_id),
                              5),
                        0) cert_brand_content_avg,
               coalesce(round(avg(round((t.fans_num - t.false_fans_num) /
                                        t.fans_num,
                                        5)) over(PARTITION BY ic.f_cert_id),
                              5),
                        0) high_cert_fans_avg_rate,
               '${dayBeforeYesterday}',
               ${cycle}
          FROM (SELECT *
                  FROM bigdata.douyin_advert_kol_calc_data
                 WHERE dt = '${dayBeforeYesterday}'
                   AND cycle = ${cycle}) t
          LEFT JOIN (SELECT d1.kol_id,
                            count(1) mention_content_num,
                            sum(new_interact_num) mention_content_interact_num,
                            round(avg(new_interact_num), 5) mention_content_interact_avg
                       FROM (SELECT kol_id, content_id
                               FROM bigdata.douyin_advert_source_keywords
                              WHERE dt > '${date_reduce}'
                                AND dt <= '${dayBeforeYesterday}'
                                AND to_date(source_date) > '${date_reduce}'
                              GROUP BY kol_id, content_id) d1
                       LEFT JOIN bigdata.douyin_advert_content_calc_data d2
                         ON d1.content_id = d2.content_id
                        AND d2.dt = '${dayBeforeYesterday}'
                        AND d2.cycle = ${cycle}
                      GROUP BY d1.kol_id) d
            ON t.kol_id = d.kol_id
          LEFT JOIN bigdata.douyin_advert_kol_property_brand_industry bi
            ON t.kol_id = bi.kol_id
           AND bi.dt = '${dayBeforeYesterday}'
           AND bi.cycle = ${cycle}
          LEFT JOIN (SELECT ic1.kol_id,
                            split(ic2.path, ',') interest_class,
                            split(ic3.path, ',') cert_label,
                            ic3.cert_1 f_cert_id,
                            ic4. NAME f_cert_name
                       FROM (select kol_id,interest_id,cert_label_id
                             from bigdata.advert_douyin_kol_mark_daily_snapshot
                             where dt='${yesterday}') ic1,
                            bigdata.advert_interest            ic2,
                            bigdata.advert_cert                ic3,
                            bigdata.advert_cert                ic4
                      WHERE ic1.interest_id = ic2.id
                        AND ic1.cert_label_id = ic3.id
                        AND ic3.cert_1 = ic4.id) ic
            ON t.kol_id = ic.kol_id
          LEFT JOIN bigdata.douyin_advert_kol_snapshot u
            ON t.kol_id = u.user_id
           AND u.dt = '${dayBeforeYesterday}';"

    executeHiveCommand "${COMMON_VAR}${es_sql}"
done