#!/bin/sh
export LANG=en_US.UTF-8
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

lib_path=/data/service/azkaban-3.38.0/azkaban-script/public/libs

day=$1

email_subject="[要][${day}][映客相关统计数据]"
email_content="数据详见附件"
#email_to="deepinsight@haima.me,wangyue@haima.me,taojiqiang@haima.me,shaolimin@haima.me,liuhong@haima.me,tanxi@haima.me,suyuanyuan@haima.me,sunli@haima.me,lining@haima.me,zhaoyimin@haima.me"
email_to="sunli@haima.me,caoshengwen@haima.me"

tmp_dir='/tmp/ingkee'

filePath=${tmp_dir}/*${day}.csv
files=""
for file in $(ls ${filePath})
do
zip -j ${file}.zip ${file}
rm ${file}
files=${files},${file}.zip
done

files=${files#*,}
echo "files:${files}"

echo "############### 映客相关统计数据邮件发送 start #####################"

java -jar ${lib_path}/com.haima.email-2.0.jar "${email_subject}" "${email_content}" "${email_to}" "${files}"


echo "############### 映客相关统计数据邮件发送 end #####################"
