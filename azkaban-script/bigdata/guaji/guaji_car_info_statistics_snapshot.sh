#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
last_week=`date -d "-7 day $date" +%Y-%m-%d`
three_day=`date -d "-3 day $date" +%Y-%m-%d`
last_month=`date -d "-30 day $date" +%Y-%m-%d`
months=`date -d "$date" +%Y-%m`

# --------------------------------- #
# -- 平台数据统计表
# --------------------------------- #
guaji_stat="insert overwrite table bigdata.guaji_car_info_statistics_snapshot
select
    '${date}' dt,
    a.sale_num_daily,b.sale_num_snapshot,c.incr_num,d.strict_num,
    e.sold_num,f.sold_val,g.sold_avg,h.charge,
    i.reserve_daily,j.reserve_all,k.audit_daily,l.audit_all,
    m.off_daily,n.off_all,
    o.tosd,p.tosw,q.tosm,r.toid,
    'car_info_statistics' meta_table_name,
    'guaji' meta_app_name
from
    (select count(distinct source_id) sale_num_daily
    from bigdata.guaji_car_info_daily_origin
    where dt='${date}' and meta_table_name='car_info'
        and car_uid not like '%unknown') a,
    (select count(source_id) sale_num_snapshot
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and state=0) b,
    (select count(source_id) incr_num
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and on_shelf='${date}' and state=0) c,
    (select count(source_id) strict_num
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and on_shelf='${date}' and state=0 and is_strict=1) d,
    (select count(source_id) sold_num
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and sold_date='${date}' and state=2) e,
    (select sum(price) sold_val
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and sold_date='${date}' and state=2) f,
    (select sum(price)/count(source_id) sold_avg
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and sold_date='${date}' and state=2 and price!=0 and price is not null) g,
    (select sum(service_charge) charge
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and sold_date='${date}' and state=2) h,
    (select count(source_id) reserve_daily
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and reserve_date='${date}' and state=1) i,
    (select count(source_id) reserve_all
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and state=1) j,
    (select count(source_id) audit_daily
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and reserve_audit_date='${date}' and state=5) k,
    (select count(source_id) audit_all
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and state=5) l,
    (select count(source_id) off_daily
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and off_shelf='${date}' and (state=3 or state=9)) m,
    (select count(source_id) off_all
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and (state=3 or state=9)) n,
    (select sum(datediff(sold_date,on_shelf))/count(1) tosd
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and sold_date='${date}' and state=2) o,
    (select sum(datediff(sold_date,on_shelf))/count(1) tosw
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and sold_date>='${last_week}' and state=2) p,
    (select sum(datediff(sold_date,on_shelf))/count(1) tosm
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and sold_date>='${last_month}' and state=2) q,
    (select sum(datediff(last_date,on_shelf))/count(1) toid
    from bigdata.guaji_car_info_all_snapshot
    where dt='${date}' and state=0) r
    union all
    select dt,
        sale_num_daily,sale_num_snapshot,incr_num,strict_num,
        sold_num,sold_val,sold_avg,charge,
        reserve_daily,reserve_all,audit_daily,audit_all,
        off_daily,off_all,
        tosd,tosw,tosm,toid,
        meta_table_name,meta_app_name
    from bigdata.guaji_car_info_statistics_snapshot;"

# ---------------------- #
# -- 同步ES
# if((a.sale_num_daily/b.sale_num_snapshot)>1,1,round(a.sale_num_daily/b.sale_num_snapshot,2)) update_rate_day
# ---------------------- #
add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_common="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

to_es="insert into table bigdata.guaji_es_car_info_statistics_snapshot
select 
    concat('car_info_statistics',dt) es_id,    
    sale_num_daily,sale_num_snapshot,incr_num,strict_num,
    sold_num,sold_val,sold_avg,charge,
    reserve_daily,reserve_all,audit_daily,audit_all,
    off_daily,off_all,
    tosd,tosw,tosm,toid,
    meta_table_name,meta_app_name,
    dt,substr(dt,0,7) months
from bigdata.guaji_car_info_statistics_snapshot
where dt='${date}';"


# ------- 运维指标写入ES ------ #

guaji_indictor="insert into table bigdata.guaji_es_car_info_indicator_snapshot
select
    concat('${date}','-0010-car_info_statistics',) es_id,
    '当天日更率' as name,
    round(a.sale_num_daily/b.sale_num_snapshot,2) value,
    '${date}' dt,
    '$months' months
from
    (select sale_num_daily
    from bigdata.guaji_car_info_statistics_snapshot
    where dt='${date}') a,
    (select count(source_id) sale_num_snapshot
    from bigdata.guaji_car_info_all_snapshot
    where dt='${yesterday}' and state=0) b

union all
select
    concat('${date}','-0020-car_info_statistics') es_id,
    '三天日更率' as name,
    round(a.sale_num_daily/b.sale_num_snapshot,2) value,
    '${date}' dt,
    '$months' months
from
    (select count(1) sale_num_daily
    from (
        select source_id
        from bigdata.guaji_car_info_daily_origin
        where dt>='${three_day}' and meta_table_name='car_info'
            and car_uid not like '%unknown'
        group by source_id) a1) a,
    (select count(source_id) sale_num_snapshot
    from bigdata.guaji_car_info_all_snapshot
    where dt='${yesterday}' and state=0) b;"



# ============================================= 函数执行 =================================================== #


echo "最终结果计算, BEGIN..."
executeHiveCommand "${guaji_stat}"
echo "最终结果计算, END!"

echo "同步到ES, BEGIN..."
executeHiveCommand "${add_es_hadoop} ${add_common} ${to_es}"
echo "同步到ES, END!"




