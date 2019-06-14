#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_calc_data.sh,douyin_advert_content_calc_data.sh
# 导出到ES 互动画像 活跃度 activity_rate  1：点赞    2：评论    3：潜水

today=$1

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

for cycle in 7 30 60
do
    echo "++++++++++++++++++++++++++++++++导出互动画像 品牌活跃度到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    date_reduce=`date -d "-${cycle} day $dayBeforeYesterday" +%Y-%m-%d`
    search_type=1
    brand_es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
        with t_orgin as ( select t4.brand_1,t6.category_id,t6.brand_id,t4.content_id from ( select t1.brand_1,t1.content_id,split(t2.path,',') brands,split(t3.path,',') categorys from (select category_id,brand_id,content_id,brand_1 from bigdata.douyin_advert_source_keywords where dt<='${dayBeforeYesterday}' and dt>'${date_reduce}' and to_date(source_date)>'${date_reduce}' group by brand_1,category_id,brand_id,content_id) t1 inner join (select * from bigdata.advert_brand where dt='${dayBeforeYesterday}') t2 on t1.brand_id=t2.id inner join (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t3 on t1.category_id=t3.id ) t4 , bigdata.douyin_advert_pbrand_brand_data t6 where array_contains(t4.brands,t6.brand_id) and array_contains(t4.categorys,t6.category_id) )
        insert into bigdata.advert_douyin_industrybrand_personas_es partition(dt='${dayBeforeYesterday}',cycle=${cycle},search_type=${search_type})
        select '${stat_date}' AS stat_month, unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,'douyin' platform, null top_industry, t.brand_1 top_brand,category_id,t.brand_id,'activity_rate',case d.type when 1 then '点赞率' when 2 then '点评率' else '潜水率' end type, case d.type when 1 then if(sum_like=0,0,round(sum_like/sum_total,5)) when 2 then if(sum_interact=0,0,round(sum_interact/sum_total,5)) when 3 then if(sum_cover=0,0,round(sum_cover/sum_total,5)) end sum_rate, case d.type when 1 then if(avg_like=0,0,round(avg_like/avg_total,5)) when 2 then if(avg_interact=0,0,round(avg_interact/avg_total,5)) when 3 then if(avg_cover=0,0,round(avg_cover/avg_total,5)) end avg_rate,'${dayBeforeYesterday}',${cycle},${search_type} from ( select brand_1,category_id,brand_id,sum(new_like_count) sum_like,sum(cover_fans_num) sum_cover,sum(interact_fans_num) sum_interact, sum(new_like_count)+sum(cover_fans_num)+sum(interact_fans_num) sum_total,round(avg(sum(new_like_count))over(partition by t1.brand_1)) avg_like, round(avg(sum(cover_fans_num))over(partition by t1.brand_1)) avg_cover,round(avg(sum(interact_fans_num))over(partition by t1.brand_1)) avg_interact, round(avg(sum(new_like_count))over(partition by t1.brand_1))+round(avg(sum(cover_fans_num))over(partition by t1.brand_1))+round(avg(sum(interact_fans_num))over(partition by t1.brand_1)) avg_total from t_orgin t1 inner join bigdata.douyin_advert_content_calc_data t2 on t1.content_id=t2.content_id and t2.dt='${dayBeforeYesterday}' and t2.cycle=${cycle} group by t1.brand_1,t1.category_id,t1.brand_id) t, (select type from (select array(1,2,3) type_arr) tm LATERAL VIEW explode(tm.type_arr) v_1 as type) d;"
    executeHiveCommand "${COMMON_VAR}${brand_es_sql}"

    echo "++++++++++++++++++++++++++++++++导出互动画像 品类活跃度到ES 统计周期'${cycle}'++++++++++++++++++++++++++++++++++++++"
    search_type=2
    category_es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
        WITH t_orgin AS ( SELECT t4.pl_1, t6.category_id, t4.content_id FROM ( SELECT t1.pl_1, t1.content_id, split (t3.path, ',') categorys FROM ( SELECT category_id, content_id, pl_1 FROM bigdata.douyin_advert_source_keywords WHERE dt <= '${dayBeforeYesterday}' AND dt > '${date_reduce}' AND to_date(source_date) > '${date_reduce}' GROUP BY pl_1, category_id, content_id ) t1 INNER JOIN (select * from bigdata.advert_category where dt='${dayBeforeYesterday}') t3 ON t1.category_id = t3.id ) t4, bigdata.douyin_advert_pbrand_brand_data t6 WHERE array_contains (t4.categorys,t6.category_id) )
        insert into bigdata.advert_douyin_industrybrand_personas_es partition(dt='${dayBeforeYesterday}',cycle=${cycle},search_type=${search_type})
        select '${stat_date}' AS stat_month, unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000, 'douyin' platform, pl_1 top_industry,null top_brand,category_id,null brand_id,'activity_rate', case d.type when 1 then '点赞率' when 2 then '点评率' else '潜水率' end type, case d.type when 1 then if(sum_like=0,0,round(sum_like/sum_total,5)) when 2 then if(sum_interact=0,0,round(sum_interact/sum_total,5)) when 3 then if(sum_cover=0,0,round(sum_cover/sum_total,5)) end sum_rate, case d.type when 1 then if(avg_like=0,0,round(avg_like/avg_total,5)) when 2 then if(avg_interact=0,0,round(avg_interact/avg_total,5)) when 3 then if(avg_cover=0,0,round(avg_cover/avg_total,5)) end avg_rate,'${dayBeforeYesterday}',${cycle},${search_type} from ( select pl_1,category_id,sum(new_like_count) sum_like,sum(cover_fans_num) sum_cover,sum(interact_fans_num) sum_interact, sum(new_like_count)+sum(cover_fans_num)+sum(interact_fans_num) sum_total,round(avg(sum(new_like_count))over(partition by t1.pl_1)) avg_like, round(avg(sum(cover_fans_num))over(partition by t1.pl_1)) avg_cover,round(avg(sum(interact_fans_num))over(partition by t1.pl_1)) avg_interact, round(avg(sum(new_like_count))over(partition by t1.pl_1))+round(avg(sum(cover_fans_num))over(partition by t1.pl_1))+round(avg(sum(interact_fans_num))over(partition by t1.pl_1)) avg_total from t_orgin t1 inner join bigdata.douyin_advert_content_calc_data t2 on t1.content_id=t2.content_id and t2.dt='${dayBeforeYesterday}' and t2.cycle=${cycle} group by t1.pl_1,t1.category_id) t, (select type from (select array(1,2,3) type_arr) tm LATERAL VIEW explode(tm.type_arr) v_1 as type) d;"
    executeHiveCommand "${COMMON_VAR}${category_es_sql}"
done