#!/bin/sh
source /etc/profile
#source /home/hadoop/yy/env.conf
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_commen="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

#--------------------------
#-- 付费用户日统计表，日明细表
#-- 单单使用累计表会删除当天的付费明细
#-- 该user_id 为直播间ID
#--------------------------
payer_info_daily="insert into bigdata.YY_live_payer_info_daily_snapshot partition (dt='${date}')
    select
        audience_id,user_id,sum(gift_val) gift_val,pay_type,sum(gift_num) gift_num
    from
        bigdata.yy_gift_info_all_tmp
    group by audience_id,user_id,pay_type;"


#----------------------------
#-- 累计付费用户数据，全量按天分区
#----------------------------
payer_info_all="insert into table bigdata.yy_live_payer_info_all_snapshot partition (dt='${date}')
select update_date,audience_id,audience_name,pay_type,data_generate_time
from
(select
  *,row_number () over (partition by audience_id order by data_generate_time desc) rowid
 from(
   select dt update_date,audience_id,audience_name,1 as pay_type,data_generate_time
   from bigdata.YY_live_danmu_data_origin
   where
        dt='${date}' and gift_num > 0 and audience_id is not null
   union all
    select update_date,audience_id,audience_name,pay_type,data_generate_time
    from bigdata.yy_live_payer_info_all_snapshot
    where dt='${yesterday}'
 ) a
) t
 where t.rowid = 1;"

echo -n "付费用户数据写入..."
executeHiveCommand "${payer_info_daily} ${payer_info_all}"
echo "付费用户数据写入,OK"

# ---------------------- +
# 日付费用户数据同步ES
# ---------------------- +

payer_daily_to_es="insert into table bigdata.yy_es_live_payer_info_daily_snapshot
     select concat(audience_id,：：：'-',user_id,'-payer_info_daily-',dt) es_id,
        audience_id,user_id,gift_val,pay_type,
        'payer_info_daily' as meta_table_name,
        'com.duowan.mobile' as meta_app_name,
        dt,gift_num,substr(dt,0,7) months
     from bigdata.yy_live_payer_info_daily_snapshot
     where dt='${date}';"

# ---------------------- +
# 累计付费用户数据同步ES
# ---------------------- +
payer_info_all_to_es="insert into table bigdata.yy_es_live_payer_info_all_snapshot
    select update_date,concat(audience_id,'-payer_info_all-',dt) es_id,
           audience_id,audience_name,pay_type,
           'payer_info_all' as meta_table_name,
           'com.duowan.mobile' as meta_app_name,
           dt,substr(dt,0,7) months
    from  bigdata.yy_live_payer_info_all_snapshot
    where dt='${date}';"


echo -n "同步到ES... "
executeHiveCommand "${add_es_hadoop} ${add_commen} ${payer_daily_to_es} ${payer_info_all_to_es}"
echo "同步到ES,OK"
