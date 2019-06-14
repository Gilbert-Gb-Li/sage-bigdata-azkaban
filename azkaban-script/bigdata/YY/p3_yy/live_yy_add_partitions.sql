
ALTER TABLE bigdata.YY_tbl_ex_live_danmu_data_origin ADD IF NOT EXISTS PARTITION(dt='${hivevar:dt}', hour='${hivevar:hour}')
location '/data/yy/origin/live_danmu/';

ALTER TABLE bigdata.YY_tbl_ex_live_gift_info_data_origin ADD IF NOT EXISTS PARTITION(dt='${hivevar:dt}', hour='${hivevar:hour}')
location '/data/yy/origin/live_gift_info/${hivevar:local}';

ALTER TABLE bigdata.YY_tbl_ex_live_id_list_data_origin ADD IF NOT EXISTS PARTITION(dt='${hivevar:dt}', hour='${hivevar:hour}')
location '/data/yy/origin/live_id_list/${hivevar:local}';

ALTER TABLE bigdata.YY_tbl_ex_live_user_info_data_origin ADD IF NOT EXISTS PARTITION(dt='${hivevar:dt}', hour='${hivevar:hour}')
location '/data/yy/origin/live_user_info/${hivevar:local}';


-- hive -hivevar dt='2019-01-05' -hivevar hour='10' -hivevar local='2019-01-05/10' -f live_yy_add_partitions.sql