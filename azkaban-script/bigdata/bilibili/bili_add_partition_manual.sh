#!bin/bash

p3_location_origin='/data/bili/origin'
day=$1


user_info_origin=" alter table bigdata.bili_live_user_info_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_info/$day'; "
gift_info_origin=" alter table bigdata.bili_live_gift_info_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_gift_info/$day'; "
guard_list_origin=" alter table bigdata.bili_live_guard_list_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_guard_list/$day'; "
contribution_origin=" alter table bigdata.bili_live_contribution_rank_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_contribution_rank/$day'; "
live_id_list=" alter table bigdata.bili_live_id_list_data_origin add if not exists partition (dt='$day') location '${p3_location_origin}/live_id_list/$day'; "

echo "##############  hive表加载数据 start ##################"
for hour in $(seq 0 23)
do
if [ $hour -lt 10 ] ;then
hour="0${hour}"
elif [ $hour -eq 21 ] ;then
echo "##############添加分区   ${user_info_origin}    ###########"
/usr/bin/hive -e  "${user_info_origin}"
echo "##############添加分区   ${gift_info_origin}   ###########"
/usr/bin/hive -e  "${gift_info_origin}"
echo "##############添加分区   ${guard_list_origin}   ###########"
/usr/bin/hive -e  "${guard_list_origin}"
echo "##############添加分区   ${contribution_origin}   ###########"
/usr/bin/hive -e  "${contribution_origin}"
echo "##############添加分区   ${live_id_list}   ###########"
/usr/bin/hive -e  "${live_id_list}"
fi
danmu_origin=" alter table bigdata.bili_live_danmu_data_origin add if not exists partition (dt='$day',hour='$hour') location '${p3_location_origin}/live_danmu_info/$day/$hour'; "
echo "##############添加分区   ${danmu_origin}   ###########"
/usr/bin/hive -e  "${danmu_origin}"
done

echo "##############   hive表加载数据 end  ##################"

