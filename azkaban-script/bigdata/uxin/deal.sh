param="2019-01-25"
sh uxin_car_info_origin_add_partition.sh ${param} 01
sh uxin_car_parameter_origin_add_partition.sh ${param} 01


sh uxin_car_parameter_snapshot_blood.sh ${param} 01
sh uxin_car_payment_snapshot_blood.sh ${param} 01
sh uxin_car_price_analysis_snapshot_blood.sh ${param} 01


sh uxin_car_info_new_snapshot_blood.sh ${param} 01
sh uxin_car_off_sale_snapshot_blood.sh ${param} 01
sh uxin_car_had_sale_snapshot_blood.sh ${param} 01


sh uxin_car_info_all_snapshot_blood.sh ${param} 01
sh uxin_car_info_all_snapshot_es_blood.sh ${param} 01