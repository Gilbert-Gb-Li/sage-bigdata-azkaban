#!/bin/sh
source /etc/profile
source ${AZKABAN_HOME}/conf/env.conf
source ${base_path}/util.sh

data_arr="ias-live ias-monitor"

delete_es_expire_data "$data_arr" $1 $3
