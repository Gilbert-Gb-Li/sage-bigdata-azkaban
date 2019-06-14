#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 依赖 douyin_advert_brand_keywords_snapshot.sh,douyin_advert_categroy_keywords_snapshot.sh
# 依赖的表 bigdata.advert_brand,bigdata.advert_category

today=$1
yesterday=`date -d "-1 day $today" +%Y-%m-%d`

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_sql1="INSERT overwrite table bigdata.douyin_advert_source_keywords PARTITION(dt='${yesterday}')
select a.keyword_id, a.brand_id,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.category_id ELSE a.category_id END category_id,
   a.keyword,c.name,d.name, a.type, a.short_video_id, a.comment_id, a.kol_id, a.source_time, a.video_create_time,
   a.comment_create_time, a.keyword_num,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_1 ELSE a.pl_1 END pl_1,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_2 ELSE a.pl_2 END pl_2,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_3 ELSE a.pl_3 END pl_3,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_4 ELSE a.pl_4 END pl_4,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_5 ELSE a.pl_5 END pl_5,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_6 ELSE a.pl_6 END pl_6,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_7 ELSE a.pl_7 END pl_7,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_8 ELSE a.pl_8 END pl_8,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_9 ELSE a.pl_9 END pl_9,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.pl_10 ELSE a.pl_10 END pl_10, a.brand_1,
   a.brand_2, a.brand_3, a.brand_4, a.brand_5, a.brand_6, a.brand_7,a.brand_8,a.brand_9,a.brand_10, a.brand_path,
   CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.category_path ELSE a.category_path END category_path
from (SELECT resource_id,keyword_id, brand_id, category_id, keyword, type, short_video_id, comment_id, kol_id,
             source_time, video_create_time, comment_create_time, keyword_num, pl_1, pl_2, pl_3, pl_4, pl_5, pl_6,
             pl_7, pl_8, pl_9, pl_10, brand_1, brand_2, brand_3, brand_4, brand_5, brand_6, brand_7,brand_8,brand_9,
             brand_10, concat_ws( ',', if(length(brand_1)=0,null,brand_1) , if(length(brand_2)=0,null,brand_2) ,
            if(length(brand_3)=0,null,brand_3) , if(length(brand_4)=0,null,brand_4) ,
            if(length(brand_5)=0,null,brand_5) , if(length(brand_6)=0,null,brand_6) ,
            if(length(brand_7)=0,null,brand_7) , if(length(brand_8)=0,null,brand_8) ,
            if(length(brand_9)=0,null,brand_9), if(length(brand_10)=0,null,brand_10) ) AS brand_path,
            concat_ws( ',', if(length(pl_1)=0,null,pl_1) , if(length(pl_2)=0,null,pl_2) ,
            if(length(pl_3)=0,null,pl_3) , if(length(pl_4)=0,null,pl_4) , if(length(pl_5)=0,null,pl_5) ,
            if(length(pl_6)=0,null,pl_6) , if(length(pl_7)=0,null,pl_7) , if(length(pl_8)=0,null,pl_8) ,
            if(length(pl_9)=0,null,pl_9) , if(length(pl_10)=0,null,pl_10) ) AS category_path
      FROM bigdata.douyin_advert_brand_keywords_snapshot
      WHERE brand_id is not null and dt = '${yesterday}' ) a
left join
     (SELECT category_id, concat_ws( ',', if(length(pl_1)=0,null,pl_1) , if(length(pl_2)=0,null,pl_2) ,
             if(length(pl_3)=0,null,pl_3) , if(length(pl_4)=0,null,pl_4) , if(length(pl_5)=0,null,pl_5) ,
             if(length(pl_6)=0,null,pl_6) , if(length(pl_7)=0,null,pl_7) , if(length(pl_8)=0,null,pl_8) ,
             if(length(pl_9)=0,null,pl_9) , if(length(pl_10)=0,null,pl_10) ) AS category_path, resource_id,
             pl_1, pl_2, pl_3, pl_4, pl_5, pl_6, pl_7, pl_8, pl_9, pl_10,type
     FROM bigdata.douyin_advert_categroy_keywords_snapshot
     WHERE dt = '${yesterday}' ) b
ON a.resource_id = b.resource_id AND a.type = b.type
left join (select * from bigdata.advert_brand where dt='${yesterday}') c
on a.brand_id=c.id
left join (select * from bigdata.advert_category where dt='${yesterday}') d
on (CASE WHEN b.category_path LIKE concat(a.category_path, '%') THEN b.category_id ELSE a.category_id END)=d.id;"

executeHiveCommand "${COMMON_VAR}${hive_sql1}"

