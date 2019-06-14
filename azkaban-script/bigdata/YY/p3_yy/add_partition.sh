#!bin/bash

p3_location_origin='/data/yy/origin'
day='2019-01-15'

echo "##############  hive表加载数据 start ##################"


for hour in $(seq 0 23)
do
if [ $hour -lt 10 ]
then
    tbl_ex_live_id_list_data_origin=" alter table bigdata.YY_live_id_list_data_origin add if not exists partition (dt='$day',hour='0$hour') location '${p3_location_origin}/live_id_list/$day/0$hour'; "

	/usr/bin/hive -e "${tbl_ex_live_id_list_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_id_list_data_origin}    ###########"

    tbl_ex_live_danmu_data_origin=" alter table bigdata.YY_live_danmu_data_origin add if not exists partition (dt='$day',hour='0$hour') location '${p3_location_origin}/live_danmu/$day/0$hour'; "

	/usr/bin/hive -e "${tbl_ex_live_danmu_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_danmu_data_origin}   ###########"

    tbl_ex_live_user_info_data_origin=" alter table bigdata.YY_live_user_info_data_origin add if not exists partition (dt='$day',hour='0$hour') location '${p3_location_origin}/live_user_info/$day/0$hour'; "
    
	/usr/bin/hive -e "${tbl_ex_live_user_info_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_user_info_data_origin}   ###########"

    tbl_ex_live_gift_info_data_origin=" alter table bigdata.YY_live_gift_info_data_origin add if not exists partition (dt='$day',hour='0$hour') location '${p3_location_origin}/live_gift_info/$day/0$hour'; "

	/usr/bin/hive -e "${tbl_ex_live_gift_info_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_gift_info_data_origin}   ###########"

else 
    tbl_ex_live_id_list_data_origin=" alter table bigdata.YY_live_id_list_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_id_list/$day/$hour'; "

	/usr/bin/hive -e "${tbl_ex_live_id_list_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_id_list_data_origin}    ###########"

    tbl_ex_live_danmu_data_origin=" alter table bigdata.YY_live_danmu_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_danmu/$day/$hour'; "

	/usr/bin/hive -e "${tbl_ex_live_danmu_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_danmu_data_origin}   ###########"

    tbl_ex_live_user_info_data_origin=" alter table bigdata.YY_live_user_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_user_info/$day/$hour'; "
    
	/usr/bin/hive -e "${tbl_ex_live_user_info_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_user_info_data_origin}   ###########"

    tbl_ex_live_gift_info_data_origin=" alter table bigdata.YY_live_gift_info_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_gift_info/$day/$hour'; "

	/usr/bin/hive -e "${tbl_ex_live_gift_info_data_origin}"

	echo "##############添加分区   ${tbl_ex_live_gift_info_data_origin}   ###########"
fi
done

echo "##############   hive表加载数据 end  ##################"

