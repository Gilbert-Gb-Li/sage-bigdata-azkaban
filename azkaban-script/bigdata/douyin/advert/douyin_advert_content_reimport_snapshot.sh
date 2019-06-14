#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

# 计算时间是T+2
# 依赖douyin_advert_kol_data_snapshot脚本

# azkaban上系统设置 参数日期=当前日期-1天
yesterday=$1
dayBeforeYesterday=`date -d "-1 day $yesterday" +%Y-%m-%d`
dayBeforeYesterday2=`date -d "-2 day $yesterday" +%Y-%m-%d`
maxPartitionContent=${dayBeforeYesterday2}

# 增加参数粉丝数大于等于8000的kol为有效KOL
QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"


hive_sql1="
  insert overwrite table bigdata.douyin_advert_content_reimport_snapshot partition
  (dt = '${yesterday}')
  select 
		app_version,
		app_package_name,
		record_time,
         short_video_id,
         video_create_time,
         description,
         cover_url_list,
         play_url_list,
         avatar_url,
         like_count,
         comments_count,
         share_count,
         author_id,
         challenge_id,
         challenge_name,
         content
    from (select *,
                 row_number() over(partition by p.short_video_id order by record_time desc) orderSeq
            from (select t.app_version,
						 t.app_package_name,
						 t.record_time,
                         t.short_video_id,
                         t.video_create_time,
                         t.description,
                         t.cover_url_list,
                         t.play_url_list,
                         t.avatar_url,
                         t.like_count,
                         t.comments_count,
                         t.share_count,
                         t.author_id,
                         t.challenge_id,
                         t.challenge_name,
                         t2.content
                    from (select 
								 app_version,
								 app_package_name,
								 record_time,
                                 short_video_id,
                                 video_create_time,
                                 description,
                                 cover_url_list,
                                 play_url_list,
                                 avatar_url,
                                 like_count,
                                 comments_count,
                                 share_count,
                                 author_id,
                                 challenge_id,
                                 challenge_name
                            from bigdata.douyin_video_data_origin_orc
                           where dt = '${yesterday}') t
					inner join (select *
                                from bigdata.douyin_advert_kol_data_snapshot
                               where dt = '${yesterday}' and follower_count > 600000 ) k
							on t.author_id = k.user_id
                    left join (select short_video_id, content
                                from (select *,
                                             row_number() over(partition by short_video_id order by record_time desc) as order_num
                                        from bigdata.douyin_video_voice_to_words_data_origin
                                       where dt = '${yesterday}') a
                               where a.order_num = 1) t2
                      on t.short_video_id = t2.short_video_id
                  union all
                  select 
						app_version,
						app_package_name,
						record_time,
                         short_video_id,
                         video_create_time,
                         description,
                         cover_url_list,
                         play_url_list,
                         avatar_url,
                         like_count,
                         comments_count,
                         share_count,
                         author_id,
                         challenge_id,
                         challenge_name,
                         content
                    from bigdata.douyin_advert_content_reimport_snapshot
                   where dt = '${dayBeforeYesterday}') p) f
   where f.orderSeq = 1;
"
executeHiveCommand "${COMMON_VAR}${hive_sql1}"