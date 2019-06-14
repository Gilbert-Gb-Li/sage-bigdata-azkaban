#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_pbrand_brand_data.sh,douyin_advert_content_calc_data.sh,douyin_advert_kol_property_brand_industry.sh,douyin_advert_category_volume_data.sh
# 依赖分词中间表bigdata.douyin_advert_source_keywords
# 导出到ES 品牌品类 品类统计表

yesterday=$1
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 1 7 30 60
do
    echo "++++++++++++++++++++++++++++++++导出品牌品类 品类统计表到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
            with t_orgin as
             (select t7.pid          p_category_id,
                     t6.category_id category,
                     t7.name        category_name,
                     kol_id,
                     content_id,
                     t6.brands_id
                from (select t1.content_id, split(t3.path, ',') categorys, kol_id
                        from (select category_id, brand_id, content_id, kol_id
                                from bigdata.douyin_advert_source_keywords
                               where dt <= '${dayBeforeYesterday}'
                                 and dt > '${date_reduce}'
                                 and to_date(source_date) > '${date_reduce}') t1
                       inner join (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t3
                          on t1.category_id = t3.id) t4,
                     (select category_id,collect_set(brand_id) brands_id from bigdata.douyin_advert_pbrand_brand_data group by category_id) t6
               inner join (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t7
                  on t6.category_id = t7.id
               where array_contains(t4.categorys, t6.category_id))
            insert into bigdata.advert_douyin_industrycategory_calc_es partition(dt = '${dayBeforeYesterday}', cycle = ${cycle})
            select '${stat_date}' AS stat_month,
                   unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
                   f.kol_id,
                   'douyin' platform,
                   '0' platform_kol_id,
                   u.nick_name kol_name,
                   ic.interest_class,
                   ic.cert_label,
                   u.follower_count,
                   u.age,
                   u.sex,
                   u.province,
                   u.city,
                   f.content_count kol_content_count,
                   f.interact_sum kol_interact_sum,
                   f.p_category_id,
                   f.category,
                   f.category_name,
                   f.brands_id,
                   count(f.kol_id) over(partition by f.p_category_id, f.category) kol_count,
                   sum(f.cover_fans_sum) over(partition by f.p_category_id, f.category) cover_fans_sum,
                   sum(interact_fans_sum) over(partition by f.p_category_id, f.category) interact_fans_sum,
                   if(sum(f.cover_fans_sum)
                      over(partition by f.p_category_id, f.category) = 0,
                      0,
                      round(sum(interact_fans_sum)
                            over(partition by f.p_category_id, f.category) /
                            sum(f.cover_fans_sum)
                            over(partition by f.p_category_id, f.category),
                            5)) fans_interact_rate,
                   sum(content_count) over(partition by f.p_category_id, f.category) content_count,
                   sum(f.interact_sum) over(partition by f.p_category_id, f.category) interact_sum,
                   if(sum(content_count)
                      over(partition by f.p_category_id, f.category) = 0,
                      0,
                      round(sum(f.interact_sum) over(partition by f.p_category_id,
                                 f.category) / sum(content_count)
                            over(partition by f.p_category_id, f.category),
                            5)) content_rate_avg,
                   '${dayBeforeYesterday}',
                   ${cycle}
              from (select tm.p_category_id,
                           tm.category,
                           tm.category_name,
                           tm.kol_id,
                           tm.brands_id,
                           sum(c1.cover_fans_num) cover_fans_sum,
                           sum(interact_fans_num) interact_fans_sum,
                           count(1) content_count,
                           sum(interact_num) interact_sum
                      from t_orgin tm
                     inner join bigdata.douyin_advert_content_calc_data c1
                        on tm.content_id = c1.content_id
                       and c1.dt = '${dayBeforeYesterday}'
                       and c1.cycle = ${cycle}
                     group by tm.p_category_id, tm.category, tm.category_name, tm.kol_id,tm.brands_id) f
              inner JOIN (select * from bigdata.douyin_advert_kol_snapshot where dt = '${dayBeforeYesterday}') u
                ON f.kol_id = u.user_id
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
                ON f.kol_id = ic.kol_id;"
    executeHiveCommand "${COMMON_VAR}${es_sql}"
done