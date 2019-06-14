#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

date=$1

###################################
########导入平台活跃打赏用户#########
###################################

hive_sql="
insert into bigdata.huya_live_active_pay_user_snapshot partition(dt='${date}')
select
      distinct audience_id
from bigdata.huya_live_danmu_origin_orc
where dt = '${date}'
     and danmu_type = 0
     and audience_id != ''
     and audience_id is not null
     and audience_id != 'null'
"

executeHiveCommand "${hive_sql}"

###################################
########导入平台新增打赏用户#########
###################################

hive_sql1="
insert into bigdata.huya_live_new_pay_user_snapshot partition(dt='${date}')
select 
      a.audience_id
from
(
    select 
          audience_id 
    from bigdata.huya_live_active_pay_user_snapshot
    where dt = '${date}'
) a
left join
(
    select 
          audience_id 
    from bigdata.huya_live_new_pay_user_snapshot
    where dt < '${date}'
) b
on a.audience_id = b.audience_id
where b.audience_id is null
"

executeHiveCommand "${hive_sql1}"