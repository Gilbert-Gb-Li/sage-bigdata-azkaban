param="$1"

hadoop fs -rm -R /data/boyu_jd/snapshot/goods_brand/dt=${param}
hadoop fs -rm -R /data/boyu_jd/snapshot/goods_day/dt=${param}
hadoop fs -rm -R /data/boyu_jd/snapshot/goods_mix/dt=${param}
hadoop fs -rm -R /data/boyu_jd/snapshot/goods_stats/dt=${param}

