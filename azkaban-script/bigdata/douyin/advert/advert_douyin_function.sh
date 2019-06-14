#!/bin/sh
source /etc/profile

QUEUENAME="advert"
COMMON_VAR="set tez.queue.name=${QUEUENAME};"

hive_fun="CREATE FUNCTION bigdata.word_split  AS 'com.haima.bigdata.CwsUDF' USING JAR 'hdfs:/data/lib/ansj/word-split-1.1-SNAPSHOT.jar';
CREATE FUNCTION bigdata.arraySum  AS 'udf.ArraySum' USING JAR 'hdfs:/data/lib/advert/quchong.jar';
CREATE FUNCTION bigdata.cover  AS 'udf.Cover' USING JAR 'hdfs:/data/lib/advert/quchong.jar';
CREATE FUNCTION bigdata.standardDeviation  AS 'udf.StandardDeviation' USING JAR 'hdfs:/data/lib/advert/quchong.jar';
CREATE FUNCTION bigdata.string_distinct  AS 'udf.StringDistinct' USING JAR 'hdfs:/data/lib/advert/quchong.jar';"

hive -e "${COMMON_VAR}${hive_fun}"