#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`

# ------------------------------------------------
# -- car_price 中间表
# ------------------------------------------------
price_snapshot="insert overwrite table bigdata.guaji_car_price_all_snapshot
select car_uid,car_name,price,down_payment,month_payment,
    data_generate_time,meta_table_name,meta_app_name
from(
    select *, row_number() over(partition by car_uid order by data_generate_time desc) rowid
    from(
        select car_uid,car_name,price,down_payment,month_payment,
            data_generate_time,meta_table_name,meta_app_name
        from bigdata.guaji_car_price_daily_origin
        where dt='${date}' and car_uid not like '%unknown' and car_uid not like '%\_%'
    union all
        select car_uid,car_name,price,down_payment,month_payment,
            data_generate_time,meta_table_name,meta_app_name
        from bigdata.guaji_car_price_all_snapshot) a
    ) b
where b.rowid =1;"

# ------------------------------------------------
# -- car_params 中间表
# ------------------------------------------------
last_valid_jar="add jar hdfs:/data/lib/udf/hive-udf-1.0-SNAPSHOT.jar;"
last_valid_create="create temporary function latest_valid_string as 'com.haima.sage.bigdata.hive.LatestValidStringUDAF';
create temporary function latest_valid_long as 'com.haima.sage.bigdata.hive.LatestValidLongUDAF';
create temporary function latest_valid_int as 'com.haima.sage.bigdata.hive.LatestValidIntUDAF';"

params_snapshot="insert overwrite table bigdata.guaji_car_params_all_snapshot
select car_uid,motor_type,seats,gear,fuel_type,emission_standard,
    firm,engine,emission,fuel_supply,drive,
    data_generate_time,meta_table_name,meta_app_name
from(
    select *, row_number() over(partition by car_uid order by data_generate_time desc) rowid
    from(
        select
        car_uid,
        latest_valid_string(data_generate_time,motor_type) motor_type,
        latest_valid_int(data_generate_time,cast(seats as int)) seats,
        latest_valid_string(data_generate_time,gear) gear,
        latest_valid_string(data_generate_time,fuel_type) fuel_type,
        latest_valid_string(data_generate_time,emission_standard) emission_standard,
        latest_valid_string(data_generate_time,firm) firm,
        latest_valid_string(data_generate_time,engine) engine,
        latest_valid_string(data_generate_time,engine) emission,
        latest_valid_string(data_generate_time,fuel_supply) fuel_supply,
        latest_valid_string(data_generate_time,drive) drive,
        max(data_generate_time) data_generate_time,
        max(meta_table_name) as meta_table_name,
        max(meta_app_name) as meta_app_name
        from bigdata.guaji_car_params_daily_origin
        where dt = '${date}' and car_uid not like '%unknown' and car_uid not like '%\_%'
        group by car_uid
    union all
        select car_uid,motor_type,seats,gear,fuel_type,emission_standard,
        firm,engine,emission,fuel_supply,drive,
        data_generate_time,meta_table_name,meta_app_name
        from bigdata.guaji_car_params_all_snapshot) a
    ) b
where b.rowid=1;"



# --------------------------------------------------------- #
# -- 去除可能为重跑的数据
# --------------------------------------------------------- #
pre_info_tmp="create temporary table bigdata.guaji_car_info_daily_pre_tmp as
select a.car_uid,
    a.source_id,a.car_name,a.apparent_mileage,a.price,a.service_percentage,
    a.service_charge,a.is_new,a.is_strict,a.state,a.registration_date,a.plate_address,a.car_area,
    a.data_generate_time,a.meta_table_name,a.meta_app_name,a.dt
from(
select *
from guaji_car_info_daily_origin
where dt='${date}' and state!=0) a
left join(
select car_uid
from guaji_car_info_all_snapshot
where dt='${yesterday}' and (state=2 or state=3)) b
on a.car_uid=b.car_uid
where b.car_uid is null
union all
select car_uid,
    source_id,car_name,apparent_mileage,price,service_percentage,
    service_charge,is_new,is_strict,state,registration_date,plate_address,car_area,
    data_generate_time,meta_table_name,meta_app_name,dt
from guaji_car_info_daily_origin
where dt='${date}' and state=0;"


# ------------------------------------------------
# -- car_info 临时表
# -- if((service_percentage*price/100)=0,0,cast(service_percentage*price/100 as int))
# ------------------------------------------------
info_tmp="create temporary table bigdata.guaji_car_info_daily_tmp as
select car_uid,
    source_id,car_name,apparent_mileage,price,service_percentage,
    service_charge,is_new,is_strict,state,registration_date,plate_address,car_area,
    data_generate_time,meta_table_name,meta_app_name,dt
from
(select split(car_uid,'_')[0] car_uid,
    source_id,car_name,apparent_mileage,price,service_percentage,
    service_charge,is_new,is_strict,state,registration_date,plate_address,car_area,
    data_generate_time,meta_table_name,meta_app_name,dt,
    row_number() over(partition by car_uid order by data_generate_time desc) rowid
from bigdata.guaji_car_info_daily_pre_tmp) a
where a.rowid=1;"


# -------------------------------------------------
# -- on sale car
# -- down_payment为null：有可能关联不上
# -- price,service_charge,service_percentage为0：解析规则中已经付默认值0
# -------------------------------------------------
all_info_tmp="create temporary table bigdata.guaji_car_info_all_tmp as
select a.car_uid,a.source_id,
    if(a.car_name is null,d.car_name,a.car_name) car_name,
    if(a.apparent_mileage is null,d.apparent_mileage,a.apparent_mileage) apparent_mileage,
    if(a.is_new is null,0,a.is_new) is_new,
    if(a.is_strict is null,d.is_strict,a.is_strict) is_strict,
    a.state,
    if(d.on_shelf is null,a.dt,d.on_shelf) on_shelf,
    a.dt off_shelf,
    if(a.service_percentage=0,d.service_percentage,a.service_percentage) service_percentage,
    if(a.service_charge=0,d.service_charge,a.service_charge) service_charge,
    if(a.price=0,if(b.price is null,d.price,b.price),a.price) price,
    if(b.down_payment is null,d.down_payment,b.down_payment) down_payment,
    if(b.month_payment is null,d.month_payment,b.month_payment) month_payment,
    if(c.motor_type is null,d.motor_type,c.motor_type) motor_type,
    if(c.seats is null,d.seats,c.seats) seats,
    if(c.gear is null,d.gear,c.gear) gear,
    if(c.fuel_type is null,d.fuel_type,c.fuel_type) fuel_type,
    if(c.emission_standard is null,d.emission_standard,c.emission_standard) emission_standard,
    if(c.firm is null,d.firm,c.firm) firm,
    if(c.engine is null,d.engine,c.engine) engine,
    if(c.emission is null,d.emission,c.emission) emission,
    if(c.fuel_supply is null,d.fuel_supply,c.fuel_supply) fuel_supply,
    if(c.drive is null,d.drive,c.drive) drive,
    if(a.registration_date is null,d.registration_date,a.registration_date) registration_date,
    if(a.plate_address is null,d.plate_address,a.plate_address) plate_address,
    if(a.car_area is null,d.car_area,a.car_area) car_area,
    a.data_generate_time,a.meta_table_name,a.meta_app_name
from
    (select * from bigdata.guaji_car_info_daily_tmp) a
left join bigdata.guaji_car_price_all_snapshot b
    on a.car_uid = b.car_uid
left join bigdata.guaji_car_params_all_snapshot c
    on a.car_uid = c.car_uid
left join (
    select *
    from bigdata.guaji_car_info_all_snapshot
    where dt='${yesterday}') d
    on a.car_uid=d.car_uid
left join bigdata.guaji_car_info_daily_state2 e
    on a.car_uid=e.car_uid ;"



# --------------------------------------
# -- car_info snapshort
# -- service_charge为0，使用join后的价格再计算一遍
# --------------------------------------

car_snapshot="insert into table bigdata.guaji_car_info_all_snapshot partition (dt='${date}')
select car_uid,source_id,car_name,apparent_mileage,
        is_new,is_strict,state,
        on_shelf,off_shelf,
        service_percentage,
        if(service_charge=0,cast(service_percentage*price/100 as int),service_charge) service_charge,
        price,down_payment,month_payment,
        motor_type,seats,gear,fuel_type,emission_standard,
        firm,engine,emission,fuel_supply,drive,
        registration_date,plate_address,car_area,
        data_generate_time,meta_table_name,meta_app_name
from(
    select *, row_number () over (partition by car_uid order by data_generate_time desc) row_id
    from(
        select car_uid,source_id,car_name,apparent_mileage,
            is_new,is_strict,state,
            on_shelf,off_shelf,
            service_percentage,service_charge,
            price,down_payment,month_payment,
            motor_type,seats,gear,fuel_type,emission_standard,
            firm,engine,emission,fuel_supply,drive,
            registration_date,plate_address,car_area,
            data_generate_time,meta_table_name,meta_app_name
        from bigdata.guaji_car_info_all_tmp
        where meta_table_name='car_info' and state is not null
            and car_uid not like 'HC%' and car_uid not like '%/%' and car_uid not like '%unknown'
            and source_id is not null and source_id!=''
    union all
        select car_uid,source_id,car_name,apparent_mileage,
            is_new,is_strict,state,
            on_shelf,off_shelf,
            service_percentage,service_charge,
            price,down_payment,month_payment,
            motor_type,seats,gear,fuel_type,emission_standard,
            firm,engine,emission,fuel_supply,drive,
            registration_date,plate_address,car_area,
            data_generate_time,meta_table_name,meta_app_name
         from bigdata.guaji_car_info_all_snapshot
         where dt='${yesterday}'
        ) a
    ) b
    where b.row_id=1;"


# -----------------------
# -- 同步ES
# ----------------------
add_es_hadoop="add jar hdfs:/data/lib/es/elasticsearch-hadoop-6.3.2.jar;"
add_common="add jar hdfs:/data/lib/es/commons-httpclient-3.1.jar;"

to_es="insert into table bigdata.guaji_es_car_info_all_snapshot
select
    concat(car_uid,'car_info',dt) es_id,
    car_uid,source_id,car_name,apparent_mileage,
    is_new,is_strict,state,
    on_shelf,off_shelf,
    service_percentage,service_charge,
    price,down_payment,month_payment,
    motor_type,seats,gear,fuel_type,emission_standard,
    firm,engine,emission,fuel_supply,drive,registration_date,
    plate_address,car_area,
    data_generate_time,meta_table_name,meta_app_name,dt,substr(dt,0,7) months
from
    bigdata.guaji_car_info_all_snapshot
where
    dt='${date}';"

# -------------------------
# -- 函数执行
# -------------------------

echo "创建 price, params 临时表 ..."
executeHiveCommand "${price_snapshot} ${last_valid_jar} ${last_valid_create} ${params_snapshot}"
echo "临时表创建完成！"

echo "计算累计车辆中间表 ..."
executeHiveCommand "${info_tmp} ${all_info_tmp} ${car_snapshot}"
echo "中间表计算完成！"

echo "同步到ES, BEGIN..."
executeHiveCommand "${add_es_hadoop} ${add_common} ${to_es}"
echo "同步到ES, OK!"