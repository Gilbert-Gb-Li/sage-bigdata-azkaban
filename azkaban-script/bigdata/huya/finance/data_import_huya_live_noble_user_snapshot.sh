#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day ${date}" +%Y-%m-%d`

##############################################################################################
########从当日抓取的弹幕中找存在24小时直播的房间作为非直播间用户续费贵族的依据（不需要去重）#########
##############################################################################################

hive_sql1="
select
      t1.live_user_id,
      t2.data_generate_time
from
(
    select
          a.live_user_id,
          b.user_id
    from
    (   
        select
              distinct live_user_id
        from bigdata.huya_live_danmu_origin_orc
        where dt = '${date}'
              and live_user_id is not null
              and live_user_id != ''
              and danmu_type = 1
    ) a
    join
    (
        select
              user_id,
              target_id
        from
        (
            select
                  user_id,
                  target_id,
                  row_number() over (partition by user_id order by data_generate_time desc) as order_num
            from bigdata.huya_live_id_list_origin_orc
            where dt = '${date}'
                  and user_id is not null
                  and user_id != ''
                  and target_id is not null
                  and target_id != ''
        ) t
        where t.order_num = 1
    ) b
    on a.live_user_id = b.target_id
) t1
join
(  
    select
          user_id,
          data_generate_time
    from
    (
        select
              user_id,
              data_generate_time,
              row_number() over (partition by user_id order by data_generate_time desc) as order_num
        from bigdata.huya_live_user_info_origin_orc
        where dt = '${date}' 
              and live_duration >= 1440
              and user_id is not null
              and user_id != ''
    ) t
    where t.order_num = 1 
) t2
on t1.user_id = t2.user_id
order by t2.data_generate_time desc
limit 1
;"

result=`hive -e "${hive_sql1}"`
echo $result

data=(${result//\t / })

echo ${data[0]}

###########################################################
########计算非主播间续费贵族的临时表(全网通知需要去重)#########
###########################################################

tmp_table_name_suffix1="tmp_noble_action_num_table"

# hive_sql1="
# select
#       '${date}' as start_time,
#       a.audience_id,
#       a.noble_classify,
#       a.noble_num,
#       a.order_num,
#       if((b.data_generate_time-a.data_generate_time)/60000 > 1,1,0) as noble_action_num
# from
# (
#     select 
#           data_generate_time,
#           audience_id,
#           noble_classify,
#           noble_num,
#           row_number() over (partition by audience_id,noble_classify,noble_num order by data_generate_time asc) as order_num
#     from bigdata.huya_live_danmu_origin_orc
#     where dt = '${date}'
#          and audience_id is not null
#          and audience_id != ''
#          and danmu_type = 1
# ) a
# left join
# (
#     select 
#           data_generate_time,
#           audience_id,
#           noble_classify,
#           noble_num,
#           order_num 
#     from
#     (
#         select 
#               data_generate_time,
#               audience_id,
#               noble_classify,
#               noble_num,
#               row_number() over (partition by audience_id,noble_classify,noble_num order by data_generate_time asc) - 1 as order_num
#         from bigdata.huya_live_danmu_origin_orc
#         where dt = '${date}'
#              and audience_id is not null
#              and audience_id != ''
#              and danmu_type = 1
#     ) t
#     where t.order_num > 0
# ) b
# on a.audience_id = b.audience_id
#    and a.noble_classify = b.noble_classify
#    and a.noble_num = b.noble_num
#    and a.order_num = b.order_num
# ;"

# hive_sql1="
# select
#       '${date}' as start_time,
#       audience_id,
#       noble_classify,
#       noble_num
# from
# (
#     select
#           substr(cast(data_generate_time as string),1,10) as sub_time,
#           data_generate_time,
#           audience_id,
#           noble_classify,
#           noble_num,
#           row_number() over (partition by substr(cast(data_generate_time as string),1,10),audience_id,noble_classify,noble_num order by data_generate_time desc) as order_num
#     from bigdata.huya_live_danmu_origin_orc
#     where dt = '${date}'
#          and audience_id is not null
#          and audience_id != ''
#          and danmu_type = 1
# ) t
# where t.order_num = 1
# ;"

hive_sql4="
select
      '${date}' as start_time,
      audience_id,
      noble_classify,
      noble_num
from bigdata.huya_live_danmu_origin_orc
where dt = '${date}'
     and live_user_id = '${data[0]}'
     and audience_id is not null
     and audience_id != ''
     and danmu_type = 1
;"

tmp_noble_action_num_table=`hiveSqlToTmpHive "${hive_sql4}" "${tmp_table_name_suffix1}"`

#################################################################
########统计每日续费贵族的开始时间、过期时间、以及续费月份叠加#########
#################################################################

# hive_sql2="
# insert into bigdata.huya_live_renew_noble_user_snapshot partition(dt='${date}')
# select
#       if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.start_time,
#         if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,b.start_time,b.start_time)) as start_time,
#       if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.audience_id,
#         if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,a.audience_id,b.audience_id)) as audience_id,
#       if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.noble_classify,
#         if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,a.noble_classify,b.noble_classify)) as noble_classify,
#       if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.total_noble_num,
#         if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,a.total_noble_num + b.total_noble_num,b.total_noble_num)) as total_noble_num,
#       if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.end_time,
#         if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,date_add(b.end_time,cast(a.total_noble_num as int)),b.end_time)) as end_time
# from
# (
#     select
#           start_time,
#           audience_id,
#           noble_classify,
#           sum(noble_num) as total_noble_num,
#           date_add(start_time,cast(sum(noble_num) as int)) as end_time
#     from
#     (
#         select
#               start_time,
#               audience_id,
#               noble_classify,
#               noble_num
#         from $tmp_noble_action_num_table
#         where noble_action_num = 1
#         UNION ALL
#         select
#               start_time,
#               audience_id,
#               noble_classify,
#               noble_num
#         from
#         (
#             select 
#                   start_time,
#                   audience_id,
#                   noble_classify,
#                   noble_num,
#                   row_number() over (partition by audience_id,noble_classify,noble_num order by order_num desc) as order_num
#             from $tmp_noble_action_num_table
#         ) s
#         where s.order_num = 1
#         UNION ALL
#         select
#               '${date}' as start_time,
#               audience_id,
#               noble_classify,
#               noble_num
#         from bigdata.huya_live_danmu_origin_orc
#         where dt = '${date}'
#               and danmu_type = 2
#               and is_this = 'true'
#     ) t
#     group by t.start_time,t.audience_id,t.noble_classify
# ) a
# full join
# (
#     select
#           start_time,
#           audience_id,
#           noble_classify,
#           total_noble_num,
#           end_time
#     from bigdata.huya_live_renew_noble_user_snapshot
#     where dt = '${yesterday}'
# ) b
# on a.audience_id = b.audience_id
#    and a.noble_classify = b.noble_classify
# ;"

hive_sql2="
insert into bigdata.huya_live_renew_noble_user_snapshot partition(dt='${date}')
select
      if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.audience_id,
        if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,a.audience_id,b.audience_id)) as audience_id,
      if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.noble_classify,
        if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,a.noble_classify,b.noble_classify)) as noble_classify,
      if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.start_time,
        if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,b.start_time,b.start_time)) as start_time,
      if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.total_noble_num,
        if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,a.total_noble_num + b.total_noble_num,b.total_noble_num)) as total_noble_num,
      if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is null and b.noble_classify is null,a.end_time,
        if(a.audience_id is not null and a.noble_classify is not null and b.audience_id is not null and b.noble_classify is not null,date_add(b.end_time,cast(a.total_noble_num as int)),b.end_time)) as end_time
from
(
    select
          start_time,
          audience_id,
          noble_classify,
          sum(noble_num) as total_noble_num,
          date_add(start_time,cast(sum(noble_num) as int)*30) as end_time
    from
    (
        select
              start_time,
              audience_id,
              noble_classify,
              noble_num
        from $tmp_noble_action_num_table
        UNION ALL
        select
              '${date}' as start_time,
              audience_id,
              noble_classify,
              noble_num
        from bigdata.huya_live_danmu_origin_orc
        where dt = '${date}'
              and danmu_type = 2
              and is_this = 'true'
    ) t
    group by t.start_time,t.audience_id,t.noble_classify
) a
full join
(
    select
          start_time,
          audience_id,
          noble_classify,
          total_noble_num,
          end_time
    from bigdata.huya_live_renew_noble_user_snapshot
    where dt = '${yesterday}'
) b
on a.audience_id = b.audience_id
   and a.noble_classify = b.noble_classify
;"

executeHiveCommand "${hive_sql2}"

############################################################
########根据续费贵族统计每日开通贵族的开始时间、过期时间#########
############################################################

# hive_sql3="
# insert into bigdata.huya_live_open_noble_user_snapshot
# select
#       start_time,
#       noble_classify,
#       cast(count(distinct audience_id)*0.01 as int) as open_noble_count,
#       date_add(start_time,30) as end_time
# from
# (
#     select
#           start_time,
#           audience_id,
#           noble_classify
#     from $tmp_noble_action_num_table
#     where noble_action_num = 1
#     UNION ALL
#     select
#           start_time,
#           audience_id,
#           noble_classify
#     from
#     (
#         select 
#               start_time,
#               audience_id,
#               noble_classify,
#               row_number() over (partition by audience_id,noble_classify,noble_num order by order_num desc) as order_num
#         from $tmp_noble_action_num_table
#     ) s
#     where s.order_num = 1
#     UNION ALL
#     select
#           '${date}' as start_time,
#           audience_id,
#           noble_classify
#     from bigdata.huya_live_danmu_origin_orc
#     where dt = '${date}'
#           and danmu_type = 2
#           and is_this = 'true'
# ) t
# group by t.start_time,t.noble_classify
# ;"

hive_sql3="
insert into bigdata.huya_live_open_noble_user_snapshot partition(dt='${date}')
select
      start_time,
      noble_classify,
      cast(count(distinct audience_id)*0.01 as int) as open_noble_count,
      date_add(start_time,30) as end_time
from
(
    select
          start_time,
          audience_id,
          noble_classify
    from $tmp_noble_action_num_table
    UNION ALL
    select
          '${date}' as start_time,
          audience_id,
          noble_classify
    from bigdata.huya_live_danmu_origin_orc
    where dt = '${date}'
          and danmu_type = 2
          and is_this = 'true'
) t
group by t.start_time,t.noble_classify
;"

executeHiveCommand "${hive_sql3}"

##################################
#########删除创建的临时表##########
##################################
drop_table_sql="DROP TABLE $tmp_noble_action_num_table"
executeHiveCommand "${drop_table_sql}"