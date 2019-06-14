#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="
insert into bigdata.douyin_user_daily_snapshot partition(dt='${date}')
select
      record_time,douyin_id,dynamic_count,short_video_count,like_video_count,school,location,sex,nick_name,user_id,signature,follower_count,like_count,certificate_type,certificate_info,following_count,shop_window,age,user_birthday,user_share_url,avatar_url,resource_key,province,city,app_version,app_package_name,music_count
from
(
    select
          *,row_number() over (partition by user_id order by record_time desc) as order_num
    from 
    (
        select t3.record_time,t3.douyin_id,case when t4.dynamic_count is null then t3.dynamic_count else t4.dynamic_count end dynamic_count,case when t4.short_video_count is null then t3.short_video_count else t4.short_video_count end short_video_count,case when t4.like_video_count is null then t3.like_video_count else t4.like_video_count end like_video_count,t3.school,t3.location,t3.sex,t3.nick_name,t3.user_id,t3.signature,t3.follower_count,t3.like_count,t3.certificate_type,t3.certificate_info,t3.following_count,t3.shop_window,t3.age,t3.user_birthday,t3.user_share_url,t3.avatar_url,t3.resource_key,t3.province,t3.city,t3.app_version,t3.app_package_name,t3.music_count from
        (
            select
                  t1.record_time,t1.douyin_id,t1.dynamic_count,t1.short_video_count,t1.like_video_count,t1.school,t1.location,case when t2.gender is null or t2.gender = '' then t1.sex else t2.gender end sex,t1.nick_name,t1.user_id,t1.signature,t1.follower_count,t1.like_count,t1.certificate_type,t1.certificate_info,t1.following_count,t1.shop_window,case when t1.age is null or t1.age = '' then t2.age else t1.age end age,t2.user_birthday,t2.user_share_url,t2.avatar_url,t1.resource_key,t1.province,t1.city,t1.app_version,t1.app_package_name,t1.music_count
            from
            (
                select
                      record_time,douyin_id,dynamic_count,short_video_count,like_video_count,school,location,sex,nick_name,user_id,signature,follower_count,like_count,certificate_type,certificate_info,following_count,shop_window,age,resource_key,province,city,app_version,app_package_name,music_count
                from bigdata.douyin_user_data_origin_orc 
                where dt='${date}'
                      and length(user_id) != 19
                      and user_id not like concat('%','follow','%')
                      and follower_count != -1
                      and following_count != -1
                      and like_count != -1
                      and if(certificate_type = 2,dynamic_count = -1,dynamic_count != -1)
                      and if(certificate_type = 2,short_video_count = -1,short_video_count != -1)
                      and if(certificate_type = 2,like_video_count = -1,like_video_count != -1)
                union all
                select
                      record_time,douyin_id,dynamic_count,short_video_count,like_video_count,school,location,sex,nick_name,user_id,signature,follower_count,like_count,certificate_type,certificate_info,following_count,shop_window,age,resource_key,province,city,app_version,app_package_name,'-1' as music_count
                from
                (
                    select 
                          *,row_number() over (partition by user_id order by record_time desc) as order_num
                    from bigdata.douyin_hot_recommend_video_and_user_daily_snapshot
                    where dt = '${date}'
                          and length(user_id) != 19
                          and user_id not like concat('%','follow','%')
                          and follower_count != -1
                          and following_count != -1
                          and like_count != -1
                          and if(certificate_type = 2,dynamic_count = -1,dynamic_count != -1)
                          and if(certificate_type = 2,short_video_count = -1,short_video_count != -1)
                          and if(certificate_type = 2,like_video_count = -1,like_video_count != -1)
                ) t
                where t.order_num = 1
            ) t1
            left join
            (
                select
                      record_time,author_id,user_birthday,user_share_url,avatar_url,gender,age
                from
                (
                    select
                          *,row_number() over (partition by author_id order by record_time desc) as order_num
                    from
                    (
                        select
                              record_time,author_id,user_birthday,user_share_url,avatar_url,gender,age from bigdata.douyin_video_daily_snapshot
                        where dt = '${date}'
                    ) as a
                ) as b
                where b.order_num = 1
            ) t2
            on t1.user_id = t2.author_id
        ) t3
        left join
        (
            select
                  user_id,dynamic_count,short_video_count,like_video_count
            from bigdata.douyin_user_video_count_daily_snapshot
            where dt = '${date}'
                 and dynamic_count != -1
                 and short_video_count != -1
                 and like_video_count != -1
        ) t4
        on t3.user_id = t4.user_id
        union all
        select
              record_time,douyin_id,dynamic_count,short_video_count,like_video_count,school,location,sex,nick_name,user_id,signature,follower_count,like_count,certificate_type,certificate_info,following_count,shop_window,age,user_birthday,user_share_url,avatar_url,resource_key,province,city,app_version,app_package_name,music_count
        from bigdata.douyin_user_daily_snapshot 
        where dt='${yesterday}'
    )as p
)as t
where t.order_num =1
;"

executeHiveCommand "${hive_sql}"