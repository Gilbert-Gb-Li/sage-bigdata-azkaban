#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1
hour=$2

echo '################# 直播间礼物快照表 start   ########################'

    echo '############# 拆分礼物数据 #############'

    tmp_gift_p2="CREATE TEMPORARY TABLE default.tmp_gift_p2 AS
    SELECT f.record_time,f.trace_id,f.app_package_name,f.data_generate_time,f.search_id,f.user_id,f.live_id,
           if(f.gift[0] == '\\\\N',null,f.gift[0]) AS audience_id,
           if(f.gift[1] == '\\\\N',null,f.gift[1]) AS audience_name,
           if(f.gift[2] == '\\\\N',null,f.gift[2]) AS gift_id,
           if(f.gift[3] == '\\\\N',null,f.gift[3]) AS gift_type,
           if(f.gift[4] == '\\\\N',null,f.gift[4]) AS gift_name,
           if(f.gift[5] == '\\\\N',null,f.gift[5]) AS gift_image_url,
           if(f.gift[6] == '\\\\N',null,f.gift[6]) AS gift_count,
           if(f.gift[7] == '\\\\N',null,f.gift[7]) AS gift_content,
           if(f.gift[8] == '\\\\N',null,f.gift[8]) AS gift_unit_price,
           if(f.gift[9] == '\\\\N',null,f.gift[9]) AS type,
           if(gift[3] == '\\\\N' and gift[2] == '\\\\N',null,
              if(gift[3] == '\\\\N' and gift[2] <> '\\\\N',gift[2],
                 if(gift[3] <> '\\\\N' and gift[2] == '\\\\N',gift[3],
                    if(gift[3] <> '\\\\N' and gift[2] <> '\\\\N',
                       concat(gift[3], gift[2]),
                       null
                    )
                 )
              )
           ) AS gift_type_id,row_number() over(order by f.record_time) as tmp_number
    FROM (
        SELECT record_time,trace_id,app_package_name,data_generate_time,
               search_id,user_id,live_id,split(r1.gift,'${gift_info_sep}') AS gift
        FROM ias_p2.tbl_ex_live_danmu_data_origin_orc AS t
        LATERAL VIEW explode(gift_info) r1 AS gift WHERE dt='${day}' AND hour='${hour}' AND gift_info IS NOT NULL
    ) as f ;
    "
    echo "${tmp_gift_p2}"
    echo '##########################################'

    insert_sql="INSERT INTO TABLE ias_p2.tbl_ex_live_gift_info_orc PARTITION(dt='${day}',hour='${hour}')

    SELECT g2.record_time,g2.trace_id,g2.app_package_name,'${ias_source}' AS data_source,g2.data_generate_time,g2.search_id,g2.user_id,g2.live_id,
           if(g2.audience_id is null,g2.audience_name,g2.audience_id) as audience_id,
           g2.audience_name,g2.gift_id,g2.gift_type,g2.gift_name,g2.gift_image_url,g2.gift_count,
           g2.gift_content,g2.gift_unit_price,g2.type,g2.gift_type_id,
           p2.gift_unit_val,p2.gift_unit_val*g2.gift_count as gift_val
    FROM
    (select * from default.tmp_gift_p2
     where gift_count>0 and ((gift_id!='0' and gift_id is not null) or gift_name is not null or gift_image_url is not null)
    ) g2
    LEFT JOIN
    (
       select a1.tmp_number,
              if(a1.gift_unit_val_by_gift_id is not null,a1.gift_unit_val_by_gift_id,
                 if(a2.gift_unit_val_by_gift_name is not null,a2.gift_unit_val_by_gift_name,a3.gift_unit_val_by_gift_url)
              ) as gift_unit_val
       from
       (

       SELECT g.tmp_number,max(v.gift_unit_val) as gift_unit_val_by_gift_id
       FROM
         default.tmp_gift_p2 AS g
       LEFT JOIN
         ias_p2.tbl_ex_gift_val_data AS v
       ON g.app_package_name=v.app_id AND g.gift_id=v.gift_id
       GROUP BY g.tmp_number,g.gift_id

       ) as a1
       FULL JOIN
       (SELECT g.tmp_number,max(v.gift_unit_val) as gift_unit_val_by_gift_name
       FROM
         default.tmp_gift_p2 AS g
       LEFT JOIN
         ias_p2.tbl_ex_gift_val_data AS v
       ON g.app_package_name=v.app_id AND g.gift_name=v.gift_name
       GROUP BY g.tmp_number,g.gift_name
       ) as a2
       ON a1.tmp_number=a2.tmp_number
       FULL JOIN
       (SELECT g.tmp_number,max(v.gift_unit_val) as gift_unit_val_by_gift_url
       FROM
         default.tmp_gift_p2 AS g
       LEFT JOIN
         ias_p2.tbl_ex_gift_val_data AS v
       ON g.app_package_name=v.app_id AND g.gift_image_url=v.gift_url
       GROUP BY g.tmp_number,g.gift_image_url
       ) as a3
       ON a1.tmp_number=a3.tmp_number

    ) p2
    ON g2.tmp_number=p2.tmp_number ;
    "

    executeHiveCommand "${tmp_gift_p2} ${insert_sql}"
echo '################# 直播间礼物快照表 end  ########################'
