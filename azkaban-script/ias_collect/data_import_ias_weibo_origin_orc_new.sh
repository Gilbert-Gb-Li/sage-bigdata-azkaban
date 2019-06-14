#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1
echo "##############   生成tbl_ex_weibo_user_topic_data_origin_orc_new表开始   ##################"
hive_sql="insert into ias.tbl_ex_weibo_user_topic_data_origin_orc_new PARTITION(dt='${date}')
select
  a.user_id, a.user_name
from
(
  select
    distinct user_id, user_name
  from ias.tbl_ex_weibo_user_topic_data_origin_orc
  where dt='${date}'
) a
left join ias.tbl_ex_weibo_user_topic_data_origin_orc_new b on a.user_id=b.user_id and a.user_name=b.user_name
where b.user_id is null or b.user_name is null"
executeHiveCommand "${hive_sql}"
echo "##############   生成tbl_ex_weibo_user_topic_data_origin_orc_new表结束  ##################"