#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
yesterday=`date -d "-1 day $date" +%Y-%m-%d`
apk='com.renrenche.carapp'

hive_sql="
INSERT INTO bigdata.renrenche_car_params_daily_snapshot partition(dt='${date}')
select t1.data_generate_time,t1.appPackageName,t1.car_uid,t1.car_name,
    t1.basic_motorcycle_type,t1.basic_motorcycle_price,t1.basic_manufacturer,t1.basic_engine,t1.basic_gearbox,t1.basic_displacement_l,t1.basic_length_width_height,
    t1.basic_structure,t1.basic_speed_max,t1.basic_official_speed_time,t1.basic_test_speed_time,t1.basic_test_braking_time,t1.basic_test_oil_consumption,
    t1.basic_gxb_oil_consumption,t1.basic_new_car_warranty,
    t1.body_length,t1.body_width,t1.body_height,t1.body_wheel_base,t1.body_front_gauge,t1.body_track_rear,t1.body_door_number,t1.body_seating,t1.body_fuel_tank_capacity,
    t1.body_luggage_space,t1.body_curb_weight,t1.body_min_gap,
    t1.eparam_engine_type,t1.eparam_displacement_ml,t1.eparam_air_intake_form,t1.eparam_cylinder_arrangement,t1.eparam_cylinders_numbe,t1.eparam_cylinder_valves_numbe,
    t1.eparam_compression_ratio,t1.eparam_valve_mechanism,t1.eparam_cylinder_bore,t1.eparam_cylinder_distance,t1.eparam_max_horsepower,t1.eparam_max_power,
    t1.eparam_max_power_speed,t1.eparam_max_torque,t1.eparam_max_torque_speed,t1.eparam_specific_technology,t1.eparam_fuel_numbe,t1.eparam_oil_supply_way,
    t1.eparam_cylinder_head_material,t1.eparam_cylinder_body_material,t1.eparam_fuel_form,t1.eparam_emission_standard,
    t1.gearbox_type,t1.gearbox_grade_number,
    t1.dpzx_drive_form,t1.dpzx_power_type,t1.dpzx_car_structure,t1.dpzx_front_suspension,t1.dpzx_back_suspension,t1.dpzx_wheel_drive_form,t1.dpzx_central_differential_construction,
    t1.brake_front,t1.brake_back,t1.brake_parke_type,t1.brake_front_wheel_type,t1.brake_back_wheel_type,t1.brake_backup_wheel_type
from  (
        select a.data_generate_time,a.appPackageName,a.car_uid,a.car_name,
            a.basic_motorcycle_type,a.basic_motorcycle_price,a.basic_manufacturer,a.basic_engine,a.basic_gearbox,a.basic_displacement_l,a.basic_length_width_height,
            a.basic_structure,a.basic_speed_max,a.basic_official_speed_time,a.basic_test_speed_time,a.basic_test_braking_time,a.basic_test_oil_consumption,
            a.basic_gxb_oil_consumption,a.basic_new_car_warranty,
            a.body_length,a.body_width,a.body_height,a.body_wheel_base,a.body_front_gauge,a.body_track_rear,a.body_door_number,a.body_seating,a.body_fuel_tank_capacity,
            a.body_luggage_space,a.body_curb_weight,a.body_min_gap,
            a.eparam_engine_type,a.eparam_displacement_ml,a.eparam_air_intake_form,a.eparam_cylinder_arrangement,a.eparam_cylinders_numbe,a.eparam_cylinder_valves_numbe,
            a.eparam_compression_ratio,a.eparam_valve_mechanism,a.eparam_cylinder_bore,a.eparam_cylinder_distance,a.eparam_max_horsepower,a.eparam_max_power,
            a.eparam_max_power_speed,a.eparam_max_torque,a.eparam_max_torque_speed,a.eparam_specific_technology,a.eparam_fuel_numbe,a.eparam_oil_supply_way,
            a.eparam_cylinder_head_material,a.eparam_cylinder_body_material,a.eparam_fuel_form,a.eparam_emission_standard,
            a.gearbox_type,a.gearbox_grade_number,
            a.dpzx_drive_form,a.dpzx_power_type,a.dpzx_car_structure,a.dpzx_front_suspension,a.dpzx_back_suspension,a.dpzx_wheel_drive_form,a.dpzx_central_differential_construction,
            a.brake_front,a.brake_back,a.brake_parke_type,a.brake_front_wheel_type,a.brake_back_wheel_type,a.brake_backup_wheel_type,
            row_number() over (partition by a.car_uid order by a.data_generate_time desc) as row_num
        from(
            SELECT data_generate_time,appPackageName,
                split(car_uid,'_')[0] as car_uid,car_name,
                basic_motorcycle_type,basic_motorcycle_price,basic_manufacturer,basic_engine,basic_gearbox,basic_displacement_l,basic_length_width_height,
                basic_structure,basic_speed_max,basic_official_speed_time,basic_test_speed_time,basic_test_braking_time,basic_test_oil_consumption,
                basic_gxb_oil_consumption,basic_new_car_warranty,
                body_length,body_width,body_height,body_wheel_base,body_front_gauge,body_track_rear,body_door_number,body_seating,body_fuel_tank_capacity,
                body_luggage_space,body_curb_weight,body_min_gap,
                eparam_engine_type,eparam_displacement_ml,eparam_air_intake_form,eparam_cylinder_arrangement,eparam_cylinders_numbe,eparam_cylinder_valves_numbe,
                eparam_compression_ratio,eparam_valve_mechanism,eparam_cylinder_bore,eparam_cylinder_distance,eparam_max_horsepower,eparam_max_power,
                eparam_max_power_speed,eparam_max_torque,eparam_max_torque_speed,eparam_specific_technology,eparam_fuel_numbe,eparam_oil_supply_way,
                eparam_cylinder_head_material,eparam_cylinder_body_material,eparam_fuel_form,eparam_emission_standard,
                gearbox_type,gearbox_grade_number,
                dpzx_drive_form,dpzx_power_type,dpzx_car_structure,dpzx_front_suspension,dpzx_back_suspension,dpzx_wheel_drive_form,dpzx_central_differential_construction,
                brake_front,brake_back,brake_parke_type,brake_front_wheel_type,brake_back_wheel_type,brake_backup_wheel_type
            FROM bigdata.renrenche_car_params_origin
            WHERE (dt = '${date}'
                AND appPackageName='${apk}'
                AND car_uid IS NOT NULL AND car_uid != ''
                AND car_name IS NOT NULL AND car_name != '')
            UNION
            SELECT data_generate_time,appPackageName,car_uid,car_name,
                basic_motorcycle_type,basic_motorcycle_price,basic_manufacturer,basic_engine,basic_gearbox,basic_displacement_l,basic_length_width_height,
                basic_structure,basic_speed_max,basic_official_speed_time,basic_test_speed_time,basic_test_braking_time,basic_test_oil_consumption,
                basic_gxb_oil_consumption,basic_new_car_warranty,
                body_length,body_width,body_height,body_wheel_base,body_front_gauge,body_track_rear,body_door_number,body_seating,body_fuel_tank_capacity,
                body_luggage_space,body_curb_weight,body_min_gap,
                eparam_engine_type,eparam_displacement_ml,eparam_air_intake_form,eparam_cylinder_arrangement,eparam_cylinders_numbe,eparam_cylinder_valves_numbe,
                eparam_compression_ratio,eparam_valve_mechanism,eparam_cylinder_bore,eparam_cylinder_distance,eparam_max_horsepower,eparam_max_power,
                eparam_max_power_speed,eparam_max_torque,eparam_max_torque_speed,eparam_specific_technology,eparam_fuel_numbe,eparam_oil_supply_way,
                eparam_cylinder_head_material,eparam_cylinder_body_material,eparam_fuel_form,eparam_emission_standard,
                gearbox_type,gearbox_grade_number,
                dpzx_drive_form,dpzx_power_type,dpzx_car_structure,dpzx_front_suspension,dpzx_back_suspension,dpzx_wheel_drive_form,dpzx_central_differential_construction,
                brake_front,brake_back,brake_parke_type,brake_front_wheel_type,brake_back_wheel_type,brake_backup_wheel_type
            FROM bigdata.renrenche_car_params_daily_snapshot
            WHERE (dt = '${yesterday}'
                AND car_uid IS NOT NULL
                AND car_uid != '')
        ) as a
      ) t1
where t1.row_num =1;
"

    echo '################删除hive表的分区####################'
    delete_hive_partitions="
    ALTER TABLE bigdata.renrenche_car_params_daily_snapshot DROP IF EXISTS PARTITION (dt='${date}');
    "

    echo '################删除HDFS上的数据####################'
    hdfs dfs -rm -r /data/renrenche/snapshot/renrenche_car_params_daily_snapshot/dt=${date}

executeHiveCommand "
${delete_hive_partitions}
${hive_sql}
"

