#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖于分词中间表
# 词云
# 服务端需要分组统计

today=$1

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

dayBeforeYesterday=`date -d "-1 day $today" +%Y-%m-%d`
date_reduce_60=`date -d "-60 day $dayBeforeYesterday" +%Y-%m-%d`
stat_date=`date -d "$dayBeforeYesterday" +%Y%m%d`

es_sql="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.advert_douyin_interact_brand_es partition(dt='${dayBeforeYesterday}')
select '${stat_date}' AS stat_month,
       unix_timestamp('${dayBeforeYesterday}', 'yyyy-MM-dd')*1000,
       t1.kol_id,
       t1.platform,
       t1.platform_kol_id,
       pl_1,
       pl_2,
       pl_3,
       pl_4,
       pl_5,
       pl_6,
       pl_7,
       pl_8,
       pl_9,
       pl_10,
       brand_1,
       brand_2,
       brand_3,
       brand_4,
       brand_5,
       brand_6,
       brand_7,
       brand_8,
       brand_9,
       brand_10,
       brand_id,
       brand_name,
       type,
       content_id,
       comment_id,
       case when size(challenge_ids)>=1 then challenge_ids else array() end challenge_ids,
       source_date,
       content_brand_num,
       comment_brand_num,
       '${dayBeforeYesterday}'
  from (select kol_id,
               'douyin' platform,
               '0' platform_kol_id,
               pl_1,
               pl_2,
               pl_3,
               pl_4,
               pl_5,
               pl_6,
               pl_7,
               pl_8,
               pl_9,
               pl_10,
               brand_1,
               brand_2,
               brand_3,
               brand_4,
               brand_5,
               brand_6,
               brand_7,
               brand_8,
               brand_9,
               brand_10,
               brand_id,
               brand_name,
               type,
               content_id,
               comment_id,
               '',
               source_date,
               if(type = 1 or type = 3, keywords_num, 0) content_brand_num,
               if(type = 2, keywords_num, 0) comment_brand_num
          from bigdata.douyin_advert_source_keywords
         where to_date(source_date) = '${dayBeforeYesterday}' and dt>='${dayBeforeYesterday}' and dt<='${today}') t1
  left join (select short_video_id, collect_set(challenge_id) challenge_ids
               from bigdata.douyin_advert_content_snapshot
              where video_create_time = '${dayBeforeYesterday}'
                and challenge_id is not null
                and challenge_id != ''
              group by short_video_id) t2
    on t1.content_id = t2.short_video_id;"


echo "++++++++++++++++++++++++++++++++导出 词云的基础表数据 ES++++++++++++++++++++++++++++++++++++++"
executeHiveCommand "${COMMON_VAR}${es_sql}"

