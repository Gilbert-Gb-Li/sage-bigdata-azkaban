#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

hive_sql="
insert into bigdata.douyin_video_daily_snapshot partition(dt='${date}')
select
      a.record_time,a.music_id,a.challenge_id,a.location,a.comments_count,a.description,a.commodity_id,a.like_count,a.is_advert,a.play_url_list,a.user_birthday,a.download_url_list,a.short_video_id,a.share_count,a.play_count,a.video_create_time,a.user_share_url,a.challenge_name,a.video_share_url,a.location_count,a.author_id,a.cover_url_list,a.avatar_url,a.hot_id,a.gender,a.age,b.hot_name,b.hot_type,a.resource_key,a.app_version,a.app_package_name,a.content,a.music_name from
(
    select
          record_time,music_id,challenge_id,location,comments_count,description,commodity_id,like_count,is_advert,play_url_list,user_birthday,download_url_list,short_video_id,share_count,play_count,video_create_time,user_share_url,challenge_name,video_share_url,location_count,author_id,cover_url_list,avatar_url,hot_id,gender,age,resource_key,app_version,app_package_name,content,music_name
    from
    (
        select
               *,row_number() over (partition by short_video_id,challenge_id order by record_time desc) as order_num
        from
        (
            select
                  a.record_time,a.music_id,a.challenge_id,a.location,a.comments_count,a.description,a.commodity_id,a.like_count,a.is_advert,a.play_url_list,a.user_birthday,a.download_url_list,a.short_video_id,a.share_count,a.play_count,a.video_create_time,a.user_share_url,a.challenge_name,a.video_share_url,a.location_count,a.author_id,a.cover_url_list,a.avatar_url,case when a.hot_id is null or a.hot_id = '' then b.hot_id else a.hot_id end hot_id,a.gender,a.age,a.resource_key,a.app_version,a.app_package_name,a.content,a.music_name
            from
            (
                select
                      t1.record_time,t1.music_id,t1.challenge_id,t1.location,t1.comments_count,t1.description,t1.commodity_id,t1.like_count,t1.is_advert,t1.play_url_list,t1.user_birthday,t1.download_url_list,t1.short_video_id,t1.share_count,t1.play_count,t1.video_create_time,t1.user_share_url,t1.challenge_name,t1.video_share_url,t1.location_count,t1.author_id,t1.cover_url_list,t1.avatar_url,t1.hot_id,t1.gender,t1.age,t1.resource_key,t1.app_version,t1.app_package_name,t2.content,t1.music_name
                from
                (
                    select
                          record_time,music_id,challenge_id,location,comments_count,description,commodity_id,like_count,is_advert,play_url_list,user_birthday,download_url_list,short_video_id,share_count,play_count,video_create_time,user_share_url,challenge_name,video_share_url,location_count,author_id,cover_url_list,avatar_url,hot_id,gender,age,resource_key,app_version,app_package_name,music_name
                    from bigdata.douyin_video_data_origin_orc 
                    where dt='${date}'
                ) t1
                left join
                (
                    select
                          short_video_id,content
                    from
                    (
                        select
                              *,row_number() over (partition by short_video_id order by record_time desc) as order_num
                        from bigdata.douyin_video_voice_to_words_data_origin
                        where dt = '${date}'
                    ) a
                    where a.order_num = 1
                ) t2
                on t1.short_video_id = t2.short_video_id
            ) a
            left join
            (
                select
                      distinct short_video_id,hot_id
                from bigdata.douyin_video_daily_snapshot 
                where dt='${yesterday}'
            ) b
            on a.short_video_id = b.short_video_id
            union all
            select
                  record_time,music_id,challenge_id,location,comments_count,description,commodity_id,like_count,is_advert,play_url_list,user_birthday,download_url_list,short_video_id,share_count,play_count,video_create_time,user_share_url,challenge_name,video_share_url,location_count,author_id,cover_url_list,avatar_url,hot_id,gender,age,resource_key,app_version,app_package_name,content,music_name
            from bigdata.douyin_video_daily_snapshot 
            where dt='${yesterday}'
        )as p
    )as t
    where t.order_num =1
) a
left join
(
    select
          hot_id,hot_name,hot_type
    from bigdata.douyin_hot_recommend_details_daily_snapshot
    where dt = '${date}'
) b
on a.hot_id = b.hot_id
;"

executeHiveCommand "${hive_sql}"