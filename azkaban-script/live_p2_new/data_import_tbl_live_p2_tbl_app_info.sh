#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh
source ${base_path}/live_p2_util.sh

day=$1

echo "############### 直播监控 汇总 app 信息 start #####################"

mysql_table="tbl_live_p2_app_info"


update_sql1="
  UPDATE tbl_live_p2_app_info a
  JOIN (
    SELECT biz_name,
           CONCAT('[',GROUP_CONCAT(DISTINCT CONCAT('\"',location, '\"')),']') AS location
    FROM tbl_live_p2_app_location_all
    where location!='全国' and location_p in ('全国','美国')
    GROUP BY biz_name
  ) b
  ON a.biz_name=b.biz_name
  SET a.location=b.location
"

echo ${update_sql1}

execSqlOnMysql "${update_sql1}"

update_sql2="
  UPDATE tbl_live_p2_app_info a
  JOIN (
    SELECT biz_name, CONCAT('{', GROUP_CONCAT(ip_info), '}') as ip_info
    FROM
    (
      SELECT biz_name,
             CONCAT('\"',
                    location_type,
                    '\":',
                    '[',
                    GROUP_CONCAT(
                      CONCAT(
                        '{',
                        '\"ip\":','\"',ip,'\"',
                        ',',
                        '\"location\":','\"',location,'\"',
                        '}'
                        )
                    ),
                      ']'
                   ) AS ip_info
      FROM tbl_live_p2_app_location_all
      where location!='全国' and location_p in ('全国','美国')
      GROUP BY biz_name,location_type
    ) b GROUP BY biz_name
  ) c
  ON a.biz_name=c.biz_name
  SET a.ip_info=c.ip_info
"

echo ${update_sql2}

execSqlOnMysql "${update_sql2}"


echo "################# 复制mysql数据到hive start ########################"

tmp_app_info_file=${tmpDir}/mysql_app_info_new.txt
tmp_app_location_file=${tmpDir}/mysql_app_location_new.txt
tmp_app_location_all_file=${tmpDir}/mysql_app_location_all_new.txt

app_info_table="tbl_live_p2_app_info"
app_location_table="tbl_live_p2_app_location"
app_location_all_table="tbl_live_p2_app_location_all"

echo "################# dump mysql 数据 start ########################"
rm -rf ${tmp_app_info_file}
rm -rf ${tmp_app_location_file}
rm -rf ${tmp_app_location_all_file}

${mysql} -h${mysql_host} -P3306 -u${mysql_user} -p${mysql_password} --default-character-set=utf8 -e "SELECT biz_name,name,version,version_code,download_url,size,hash,type,icon_url,location,ip_info,contact_info from ${mysql_db}.tbl_live_p2_app_info" > ${tmp_app_info_file}
${mysql} -h${mysql_host} -P3306 -u${mysql_user} -p${mysql_password} --default-character-set=utf8 -e "SELECT biz_name,name,version,version_code,location_type,location,ip from ${mysql_db}.tbl_live_p2_app_location" > ${tmp_app_location_file}
${mysql} -h${mysql_host} -P3306 -u${mysql_user} -p${mysql_password} --default-character-set=utf8 -e "SELECT biz_name,name,version,version_code,location_type,location,location_p,ip from ${mysql_db}.tbl_live_p2_app_location_all" > ${tmp_app_location_all_file}

echo "################# dump mysql 数据 end ########################"

echo "################# 创建tbl_live_p2_app_info AND tbl_live_p2_app_location AND tbl_live_p2_app_location_all table start ########################"

drop_table_app_info_table="drop table live_p2.${app_info_table};"
echo "${drop_table_app_info_table}"

create_app_info_table="
CREATE TABLE IF NOT EXISTS live_p2.${app_info_table}(
  biz_name STRING,
  name STRING,
  version STRING,
  version_code INT,
  download_url STRING,
  size INT,
  hash STRING,
  type INT,
  icon_url STRING,
  location STRING,
  ip_info STRING,
  contact_info STRING
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
"
echo "${create_app_info_table}"


drop_table_app_location_table="drop table live_p2.${app_location_table};"
echo "${drop_table_app_location_table}"

create_app_location_table="
CREATE TABLE IF NOT EXISTS live_p2.${app_location_table}(
  biz_name STRING,
  name STRING,
  version STRING,
  version_code INT,
  location_type STRING,
  location STRING,
  ip STRING
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
"
echo "${create_app_location_table}"

drop_table_app_location_all_table="drop table live_p2.${app_location_all_table};"
echo "${drop_table_app_location_all_table}"

create_app_location_all_table="
CREATE TABLE IF NOT EXISTS live_p2.${app_location_all_table}(
  biz_name STRING,
  name STRING,
  version STRING,
  version_code INT,
  location_type STRING,
  location STRING,
  location_p STRING,
  ip STRING
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE;
"
echo "${create_app_location_all_table}"

executeHiveCommand "${drop_table_app_info_table} ${create_app_info_table} LOAD DATA LOCAL INPATH '${tmp_app_info_file}' INTO TABLE live_p2.${app_info_table};
                    ${drop_table_app_location_table} ${create_app_location_table} LOAD DATA LOCAL INPATH '${tmp_app_location_file}' INTO TABLE live_p2.${app_location_table};
                    ${drop_table_app_location_all_table} ${create_app_location_all_table} LOAD DATA LOCAL INPATH '${tmp_app_location_all_file}' INTO TABLE live_p2.${app_location_all_table};"

echo "################# 创建tbl_live_p2_app_info AND tbl_live_p2_app_location AND tbl_live_p2_app_location_all table end ########################"
rm -rf ${tmp_app_info_file}
rm -rf ${tmp_app_location_file}
rm -rf ${tmp_app_location_all_file}

echo "############### 直播监控 汇总 app 信息 end #####################"
