#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
stat_date=`date -d "$date" +%Y%m%d`
stat_month=`date -d "${date}" +%Y%m`
live_interval_time=60

####################################################################
########计算主播直播次数和直播时长临时表(处星秀类以及比赛类直播)#########
####################################################################

tmp_table_name_suffix1="tmp_live_num_table"

hive_sql1="
select
      a.user_id,
      a.order_num,
      a.online_num as online_num1,
      b.online_num as online_num2,
      a.data_generate_time as live_end_time1,
      a.live_duration as live_duration1,
      a.data_generate_time-a.live_duration*60000 as live_start_time1,
      b.data_generate_time as live_end_time2,
      b.live_duration as live_duration2,
      b.data_generate_time-b.live_duration*60000 as live_start_time2,
      if(((b.live_duration - a.live_duration) < 0) or (((b.data_generate_time-a.data_generate_time)/60000 - (b.live_duration-a.live_duration)) >= 10),1,0) as live_num 
from
(
    select 
          data_generate_time,
          user_id,
          case when live_duration >= cast((data_generate_time - unix_timestamp(concat('${date}',' 00:00:00'))*1000)/60000 as int) then cast((data_generate_time - unix_timestamp(concat('${date}',' 00:00:00'))*1000)/60000 as int) else live_duration end live_duration,
          online_num,
          row_number() over (partition by user_id order by data_generate_time asc) as order_num
    from bigdata.huya_live_user_info_origin_orc
    where dt = '${date}'
         and user_id is not null
         and user_id != ''
         and is_live = 'true'
         and live_duration != 0
) a
left join
(
    select 
          data_generate_time,
          user_id,
          case when live_duration >= cast((data_generate_time - unix_timestamp(concat('${date}',' 00:00:00'))*1000)/60000 as int) then cast((data_generate_time - unix_timestamp(concat('${date}',' 00:00:00'))*1000)/60000 as int) else live_duration end live_duration,
          online_num,
          order_num 
    from
    (
        select 
              data_generate_time,
              user_id,
              live_duration,
              online_num,
              row_number() over (partition by user_id order by data_generate_time asc)  - 1 as order_num
        from bigdata.huya_live_user_info_origin_orc
        where dt = '${date}'
             and user_id is not null
             and user_id != ''
             and is_live = 'true'
             and live_duration != 0
    ) t
    where t.order_num > 0
) b
on a.user_id = b.user_id and a.order_num = b.order_num;
"

tmp_live_num_table=`hiveSqlToTmpHive "${hive_sql1}" "${tmp_table_name_suffix1}"`

###############################################################
########计算星秀类和比赛类直播主播直播次数和直播时长临时表#########
###############################################################

tmp_table_name_suffix5="tmp_live_num_table1"

hive_sql5="
select
      user_id,
      max_online_num as online_num1,
      max_online_num as online_num2,
      1 as live_num,
      split(time_str1,'-0-')[0] as live_start_time,
      split(time_str1,'-0-')[size(split(time_str1,'-0-'))-1] as live_end_time,
      cast((cast(split(time_str1,'-0-')[size(split(time_str1,'-0-'))-1] as bigint)-cast(split(time_str1,'-0-')[0] as bigint))/60000 as int) as live_duration
from
(
    select
          user_id,
          MAX(online_num) as max_online_num,
          substr(concat_ws('-',collect_set(tmp)),0,length(concat_ws('-',collect_set(tmp)))-2) as time_str
    from
    (
        select
              a.user_id,
              a.order_num as order_num,
              a.online_num as online_num,
              a.data_generate_time as live_time1,
              b.data_generate_time as live_time2,
              (b.data_generate_time-a.data_generate_time)/60000 as not_online_time,
              if((b.data_generate_time-a.data_generate_time)/60000 > 60,1,0) as live_num,
              concat(cast(a.data_generate_time as string),'-',if((b.data_generate_time-a.data_generate_time)/60000 > 60,'1','0')) as tmp
        from
        (
            select 
                  data_generate_time,
                  user_id,
                  online_num,
                  row_number() over (partition by user_id order by data_generate_time asc) as order_num
            from bigdata.huya_live_user_info_origin_orc
            where dt = '2019-05-06'
                 and user_id is not null
                 and user_id != ''
                 and is_live = 'true'
                 and live_duration = 0
        ) a
        left join
        (
            select 
                  data_generate_time,
                  user_id,
                  online_num,
                  order_num 
            from
            (
                select 
                      data_generate_time,
                      user_id,
                      online_num,
                      row_number() over (partition by user_id order by data_generate_time asc)  - 1 as order_num
                from bigdata.huya_live_user_info_origin_orc
                where dt = '2019-05-06'
                     and user_id is not null
                     and user_id != ''
                     and is_live = 'true'
                     and live_duration = 0
            ) t
            where t.order_num > 0
        ) b
        on a.user_id = b.user_id and a.order_num = b.order_num
    ) t
    group by t.user_id
) s
LATERAL VIEW explode(split(time_str,'-1-')) table1 as time_str1
"

tmp_live_num_table1=`hiveSqlToTmpHive "${hive_sql5}" "${tmp_table_name_suffix5}"`

#####################################################################
########计算主播直播次数、直播时长、开播时间、结束时间、最高在线人数#########
#####################################################################

tmp_table_name_suffix2="tmp_live_num_result_table"

hive_sql2="
select
      t1.user_id,
      t1.live_end_time,
      t1.live_duration,
      t1.live_start_time,
      live_num,
      t2.max_online_num 
from
(
    select 
          user_id,
          concat_ws(',',collect_set(cast(live_end_time as string))) as live_end_time,
          sum(live_duration) as live_duration,
          concat_ws(',',collect_set(cast(live_start_time as string))) as live_start_time,
          sum(live_num) as live_num
    from
    (
        select 
              user_id,
              live_end_time1 as live_end_time,
              live_duration1 as live_duration,
              live_start_time1 as live_start_time,
              live_num
        from $tmp_live_num_table
        where live_num = 1
        UNION ALL
        select
              user_id,
              live_end_time1 as live_end_time,
              live_duration1 as live_duration,
              live_start_time1 as live_start_time,
              1 as live_num
        from
        (
            select 
                  user_id,
                  live_end_time1,
                  live_duration1,
                  live_start_time1,
                  row_number() over (partition by user_id order by order_num desc) as order_num1 
            from $tmp_live_num_table
        ) s
        where s.order_num1 = 1
        UNION ALL
        select
              user_id,
              cast(live_end_time as bigint),
              live_duration,
              cast(live_start_time as bigint),
              live_num
        from $tmp_live_num_table1
    ) t
    group by user_id
) t1
left join
(
    select
          user_id,
          if((MAX(online_num1) > MAX(online_num2)),MAX(online_num1),MAX(online_num2)) as max_online_num
    from
    (
        select
              user_id,
              online_num1,
              online_num2
        from $tmp_live_num_table
        UNION ALL
        select
              user_id,
              online_num1,
              online_num2
        from $tmp_live_num_table1
    ) s
    group by s.user_id
) t2
on t1.user_id = t2.user_id;
"

tmp_live_num_result_table=`hiveSqlToTmpHive "${hive_sql2}" "${tmp_table_name_suffix2}"`

##################################################
########对于弹幕礼物数据涉及到连击去重中间表#########
##################################################

tmp_table_name_suffix5="tmp_live_danmu_gift_lianji_table"

hive_sql6="
select
      t1.data_generate_time as data_generate_time1,
      t2.data_generate_time as data_generate_time2,
      t1.audience_id as audience_id1,
      t2.audience_id as audience_id2,
      t1.gift_id as gift_id1,
      t2.gift_id as gift_id2,
      t1.gift_md5 as gift_md51,
      t2.gift_md5 as gift_md52,
      t1.gift_num as gift_num1,
      t2.gift_num as gift_num2,
      t1.gift_dribble as gift_dribble1,
      t2.gift_dribble as gift_dribble2,
      t1.live_user_id as live_user_id1,
      t2.live_user_id as live_user_id2,
      t1.order_num as order_num1,
      t2.order_num as order_num2,
      (t2.data_generate_time - t1.data_generate_time)/1000 as time_diff,
      if((t2.data_generate_time - t1.data_generate_time)/1000 > 30,1,0) as num
from
(
    select
          data_generate_time,
          audience_id,
          case when gift_id is null then '' else gift_id end gift_id,
          case when gift_md5 is null then '' else gift_md5 end gift_md5,
          gift_num,
          gift_dribble,
          live_user_id,
          row_number() over (partition by audience_id,live_user_id,gift_id,gift_md5,gift_num order by data_generate_time,gift_dribble asc) as order_num
    from bigdata.huya_live_danmu_origin_orc
    where dt = '${date}'
        and danmu_type = 0
) t1

left join

(
    select
          data_generate_time,
          audience_id,
          gift_id,
          gift_md5,
          gift_num,
          gift_dribble,
          live_user_id,
          order_num
    from
    (
        select
              data_generate_time,
              audience_id,
              case when gift_id is null then '' else gift_id end gift_id,
              case when gift_md5 is null then '' else gift_md5 end gift_md5,
              gift_num,
              gift_dribble,
              live_user_id,
              row_number() over (partition by audience_id,live_user_id,gift_id,gift_md5,gift_num order by data_generate_time,gift_dribble asc) - 1 as order_num
        from bigdata.huya_live_danmu_origin_orc
        where dt = '${date}'
            and danmu_type = 0
    ) t
    where t.order_num > 0
) t2

on t1.audience_id = t2.audience_id
   and
   t1.live_user_id = t2.live_user_id
   and
   t1.gift_id = t2.gift_id
   and
   t1.gift_md5 = t2.gift_md5
   and
   t1.gift_num = t2.gift_num
   and
   t1.order_num = t2.order_num
;"

tmp_live_danmu_gift_lianji_table=`hiveSqlToTmpHive "${hive_sql6}" "${tmp_table_name_suffix5}"`

##################################################
########对于弹幕礼物数据涉及到连击去重结果表#########
##################################################

hive_sql7="
insert into bigdata.huya_live_danmu_gift_result_snapshot partition(dt='${date}')
select
      data_generate_time1 as data_generate_time,
      audience_id1 as audience_id,
      gift_id1 as gift_id,
      case when gift_md51 = '' then null else gift_md51 end gift_md5,
      gift_num1 as gift_num,
      gift_dribble1 as gift_dribble,
      live_user_id1 as live_user_id
from $tmp_live_danmu_gift_lianji_table
where num = 1

UNION ALL

select
      data_generate_time,
      audience_id,
      gift_id,
      gift_md5,
      gift_num,
      gift_dribble,
      live_user_id
from
(
    select
          data_generate_time1 as data_generate_time,
          audience_id1 as audience_id,
          gift_id1 as gift_id,
          case when gift_md51 = '' then null else gift_md51 end gift_md5,
          gift_num1 as gift_num,
          gift_dribble1 as gift_dribble,
          live_user_id1 as live_user_id,
          row_number() over (partition by audience_id1,live_user_id1,gift_id1,gift_md51,gift_num1 order by order_num1 desc) as order_num
    from $tmp_live_danmu_gift_lianji_table
) t
where t.order_num = 1
;"

executeHiveCommand "${hive_sql7}"

########################################
########计算主播礼物收入、礼物支出#########
########################################

tmp_table_name_suffix3="tmp_live_danmu_gift_table"

hive_sql3="
select
      room_id,
      type,
      sum(total_money)/1000 as total_money
from
(
    select 
          a.room_id,
          a.type,
          a.gift_id,
          cast(a.real_gift_num*b.gift_gold as double) as total_money 
    from
    (
        select
              live_user_id as room_id,
              gift_id,
              sum(gift_num*gift_dribble) as real_gift_num,
              0 as type
        from 
        (
            select 
                  live_user_id,
                  gift_id,
                  gift_num,
                  case when gift_dribble = 0 then 1 else gift_dribble end gift_dribble
            from bigdata.huya_live_danmu_gift_result_snapshot
            where dt = '${date}'
                 and gift_md5 is null
            UNION ALL
            select
                  a.live_user_id,
                  b.gift_id,
                  a.gift_num,
                  a.gift_dribble
            from
            (
                select 
                      live_user_id,
                      gift_md5,
                      gift_num,
                      case when gift_dribble = 0 then 1 else gift_dribble end gift_dribble
                from bigdata.huya_live_danmu_gift_result_snapshot
                where dt = '${date}'
                     and gift_md5 is not null
            ) a
            left join
            (
                select
                      gift_id,
                      gift_md5
                from bigdata.huya_live_all_gift_snapshot
                where dt = '${date}'
            ) b
            on a.gift_md5 = b.gift_md5
        ) t
        group by live_user_id,gift_id
        UNION ALL
        select 
              audience_id as room_id,
              gift_id,
              sum(gift_num*gift_dribble) as real_gift_num,
              1 as type
        from
        (
            select 
                  audience_id,
                  gift_id,
                  gift_num,
                  case when gift_dribble = 0 then 1 else gift_dribble end gift_dribble
            from bigdata.huya_live_danmu_gift_result_snapshot
            where dt = '${date}'
                 and gift_md5 is null
            UNION ALL
            select
                  a.audience_id,
                  b.gift_id,
                  a.gift_num,
                  a.gift_dribble
            from
            (
                select 
                      audience_id,
                      gift_md5,
                      gift_num,
                      case when gift_dribble = 0 then 1 else gift_dribble end gift_dribble
                from bigdata.huya_live_danmu_gift_result_snapshot
                where dt = '${date}'
                     and gift_md5 is not null
            ) a
            left join
            (
                select
                      gift_id,
                      gift_md5
                from bigdata.huya_live_all_gift_snapshot
                where dt = '${date}'
            ) b
            on a.gift_md5 = b.gift_md5
        ) t
        group by audience_id,gift_id
    ) a
    left join
    (
        select
              distinct gift_id,gift_gold 
        from bigdata.huya_live_all_gift_snapshot
        where dt = '${date}'
    ) b
    on a.gift_id = b.gift_id
) t
group by t.room_id,t.type;
"

tmp_live_danmu_gift_table=`hiveSqlToTmpHive "${hive_sql3}" "${tmp_table_name_suffix3}"`

########################################
########计算主播送礼人数、互动人数#########
########################################

tmp_table_name_suffix4="tmp_live_danmu_user_table"

hive_sql4="
select 
      room_id,
      MAX(interact_count) as interact_count,
      MAX(contribute_count) as contribute_count 
from
(
    select 
          live_user_id as room_id,
          count(distinct audience_id) as interact_count,
          0 as contribute_count
    from bigdata.huya_live_danmu_origin_orc
    where dt = '${date}'
         and is_this = 'true'
    group by live_user_id
    union all
    select 
          live_user_id as room_id,
          0 as interact_count,
          count(distinct audience_id) as contribute_count
    from bigdata.huya_live_danmu_origin_orc
    where dt = '${date}'
         and is_this = 'true'
         and danmu_type = 0
    group by live_user_id
) s
group by room_id;
"

tmp_live_danmu_user_table=`hiveSqlToTmpHive "${hive_sql4}" "${tmp_table_name_suffix4}"`

################################
########计算活跃主播快照#########
################################

hive_sql4="
insert into bigdata.huya_live_active_user_info_snapshot partition(dt='${date}')
select 
      s1.*,
      case when s2.interact_count is null then 0 else s2.interact_count end interact_count,
      case when s2.contribute_count is null then 0 else s2.contribute_count end contribute_count,
      round(cast(s1.income as double)/contribute_count,2) as arpu
from
(
    select 
          a.*,
          case when b.total_money is null then 0 else b.total_money end pay
    from
    (
        select 
             t1.*,
             case when t2.total_money is null then 0 else t2.total_money end income 
        from
        (
            select
                  a.data_generate_time,
                  a.resource_key,
                  a.spider_version,
                  a.app_version,
                  a.user_id,
                  a.user_name,
                  a.user_age,
                  a.user_sign,
                  a.user_image,
                  a.province,
                  a.city,
                  a.user_level,
                  a.user_love_channel,
                  a.user_subscribe_num,
                  a.user_fans_num,
                  a.favor_num,
                  a.live_id,
                  a.live_desc,
                  a.user_notice,
                  a.share_url,
                  a.target_id,
                  case when b.live_start_time is null then '' else b.live_start_time end live_start_time,
                  case when b.live_end_time is null then '' else b.live_end_time end live_end_time,
                  case when b.live_duration is null then 0 else b.live_duration end live_duration,
                  case when b.live_num is null then 0 else b.live_num end live_num,
                  case when b.max_online_num is null then 0 else b.max_online_num end max_online_num
            from
            (
                select 
                      a.data_generate_time,
                      a.resource_key,
                      a.spider_version,
                      a.app_version,
                      a.user_id,
                      a.user_name,
                      b.user_sign,
                      b.user_age,
                      a.user_image,
                      b.province,
                      b.city,
                      b.user_level,
                      b.user_love_channel,
                      b.user_subscribe_num,
                      b.user_fans_num,
                      b.favor_num,
                      a.live_id,
                      a.live_desc,
                      b.user_notice,
                      a.share_url,
                      a.target_id
                from
                (
                    select 
                          data_generate_time,
                          resource_key,
                          spider_version,
                          app_version,
                          user_id,
                          user_name,
                          live_id,
                          live_desc,
                          user_image,
                          share_url,
                          target_id
                    from
                    (
                        select 
                              *,
                              row_number() over (partition by user_id order by data_generate_time desc) as order_num
                        from bigdata.huya_live_id_list_origin_orc
                        where dt = '${date}'
                             and user_id is not null
                             and user_id != ''
                    ) t
                    where t.order_num = 1
                ) a
                left join
                (
                    select 
                          data_generate_time,
                          resource_key,
                          spider_version,
                          app_version,
                          user_id,
                          user_name,
                          user_notice,
                          user_age,
                          user_sign,
                          province,
                          city,
                          user_level,
                          user_love_channel,
                          user_subscribe_num,
                          user_fans_num,
                          favor_num,
                          room_id 
                    from
                    (
                        select 
                              *,
                              row_number() over (partition by user_id order by data_generate_time desc) as order_num
                        from bigdata.huya_live_user_info_origin_orc
                        where dt = '${date}'
                             and user_id is not null
                             and user_id != ''
                    ) t
                    where t.order_num = 1
                ) b
                on a.user_id = b.user_id
            ) a
            left join
            (
                select
                      user_id,
                      live_end_time,
                      live_duration,
                      live_start_time,
                      live_num,
                      max_online_num 
                from
                $tmp_live_num_result_table
            ) b
            on a.user_id = b.user_id
        ) t1
        left join
        (
            select 
                  room_id,
                  total_money 
            from
            $tmp_live_danmu_gift_table
            where type = 0
        ) t2
        on t1.target_id = t2.room_id
    ) a
    left join
    (
        select 
              room_id,
              total_money 
        from
        $tmp_live_danmu_gift_table
        where type = 1
    ) b
    on a.target_id = b.room_id
) s1
left join
(
    select 
          room_id,
          interact_count,
          contribute_count from
    $tmp_live_danmu_user_table
) s2
on s1.target_id = s2.room_id
;"

executeHiveCommand "${hive_sql4}"

##################################
#########删除创建的临时表##########
##################################
drop_table_sql1="DROP TABLE $tmp_live_num_table"
executeHiveCommand "${drop_table_sql1}"

drop_table_sql2="DROP TABLE $tmp_live_num_result_table"
executeHiveCommand "${drop_table_sql2}"

drop_table_sql3="DROP TABLE $tmp_live_danmu_gift_table"
executeHiveCommand "${drop_table_sql3}"

drop_table_sql4="DROP TABLE $tmp_live_danmu_user_table"
executeHiveCommand "${drop_table_sql4}"

drop_table_sql5="DROP TABLE $tmp_live_num_table1"
executeHiveCommand "${drop_table_sql5}"

drop_table_sql6="DROP TABLE $tmp_live_danmu_gift_lianji_table"
executeHiveCommand "${drop_table_sql6}"

################################
########导出结果数据到ES#########
################################

hive_sql5="
add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;
add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;
insert into bigdata.huya_live_active_user_info_es_data
select
      concat('${stat_date}','_','live_user','_',user_id),
      '${stat_month}',
      unix_timestamp(dt, 'yyyy-MM-dd')*1000,
      'huya',
      'live_user',
      user_id,
      user_name,
      user_age,
      user_sign,
      user_image,
      province,
      city,
      user_level,
      user_love_channel,
      user_subscribe_num,
      user_fans_num,
      favor_num,
      live_id,
      live_desc,
      user_notice,
      share_url,
      target_id,
      live_start_time,
      live_end_time,
      live_duration,
      live_num,
      max_online_num,
      income,
      pay,
      interact_count,
      contribute_count,
      arpu
from bigdata.huya_live_active_user_info_snapshot
where dt = '${date}'
"

executeHiveCommand "${hive_sql5}"