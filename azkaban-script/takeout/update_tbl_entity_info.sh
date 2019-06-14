#!/bin/sh
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

yesterday=$1

create_tmp_mysql_table="create table tmp_entity (entity_id varchar(20))"

execSqlTakeoutOnMysql "${create_tmp_mysql_table}"

hive_sql="select distinct entity_id from takeout.tbl_ex_entity_question_info_snapshot where question_type='0' and dt='${yesterday}'"

hiveSqlToTakeoutMysqlNoDelete "${hive_sql}" "tmp_entity" "entity_id" 

update_sql="update waimai_entity set has_question=0 where entity_id in (select * from tmp_entity)"

execSqlTakeoutOnMysql "${update_sql}"

execSqlTakeoutOnMysql "drop table tmp_entity"
