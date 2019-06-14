param="$1"

hadoop fs -rm -R /data/taobao/snapshot/taobao_boyu_goods_active_snapshot/dt=${param}
hadoop fs -rm -R /data/taobao/snapshot/taobao_boyu_goods_all_snapshot/dt=${param}
hadoop fs -rm -R /data/taobao/snapshot/taobao_boyu_goods_brand_snapshot/dt=${param}
hadoop fs -rm -R /data/taobao/snapshot/taobao_boyu_goods_new_snapshot/dt=${param}
hadoop fs -rm -R /data/taobao/snapshot/taobao_boyu_goods_statistics_snapshot/dt=${param}
hadoop fs -rm -R /data/taobao/snapshot/taobao_boyu_shop_all_snapshot/dt=${param}
hadoop fs -rm -R /data/taobao/snapshot/taobao_boyu_shop_new_snapshot/dt=${param}


