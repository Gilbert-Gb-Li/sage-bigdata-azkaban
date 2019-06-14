#!/bin/sh
source /etc/profile
export LANG=en_US.UTF-8
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

#生产环境
lib_path=/data/service/azkaban-3.38.0/azkaban-script/public/libs
#开发测试环境
#lib_path=/data/service/azkaban-3.38.0/azkaban-script/public/libs

day=$1
hour=$2
echo ${day}'-'${hour}
echo "############### 直播未匹配礼物数据查询 start #####################"


file_subfix=${day}'-'${hour}
echo "file_subfix:${file_subfix}"

email_subject="[要][${file_subfix}][直播未匹配礼物数据]"
email_content="数据详见附件"
#email_to="caoshengwen@haima.me,haimacloud-de@haima.me,deepinsight@haima.me,luoyalong@haima.me,liubaoguo@haima.me,tonghejia@haima.me"
email_to="caoshengwen@haima.me"


txt_name="直播未匹配礼物数据-${file_subfix}.txt"
txt_file=${tmpDir}/live_gift_offline_moniter/${txt_name}
echo "txt_file:${txt_file}"

hive_sql="select * from (
             select '时间' as time, 'APP包名' as app_package_name,'礼物ID' as gift_id,'礼物类型' as gift_type,'礼物名称' as gift_name,'礼物图片URL' as gift_image_url,'礼物类型+礼物ID' as gift_type_id,'礼物数' as gift_count,'信息内容' as gift_content
             union
             select concat(t.dt,'-',t.hour) as time,t.app_package_name,t.gift_id,t.gift_type,t.gift_name,t.gift_image_url,t.gift_type_id,cast(t.gift_count as string) as gift_count,t.gift_content
             from(
                 select dt,hour,app_package_name,gift_id,gift_type,gift_name,gift_image_url,gift_type_id,gift_count,gift_content,
                     row_number() over (partition by app_package_name,gift_id,gift_type,gift_name,gift_image_url,gift_type_id order by gift_content desc) as order_num
                 from ias_p2.tbl_ex_live_gift_info_orc where dt='${day}' and hour='${hour}' and gift_unit_val is null
                 and ((gift_id!='0' and gift_id is not null) or gift_name is not null or gift_image_url is not null)
             ) as t where t.order_num =1
          ) t order by (case time when '时间' then 0 else 1 end);"

#temp_path=${tmpDir}/live_gift_offline_moniter/$(date "+%s%N")

`hive -e "${hive_sql}"  > ${txt_file} `

#cat ${temp_path} | sed 's/\t/,/g;s/[[:space:]]//g' > ${csv_file}

echo "TMP PATH:"${txt_file}

java -jar ${lib_path}/com.haima.email-2.0.jar "${email_subject}" "${email_content}" "${email_to}"  "${txt_file}"

#rm -rf ${txt_file}

echo "############### 直播未匹配礼物数据查询 end #####################"
