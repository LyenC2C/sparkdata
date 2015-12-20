#sh step.sh  /commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01
#sh step.sh  /commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-02
#20151101  20151102 w
#sh step.sh  /commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01
#sh step.sh  /commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01
#sh step.sh  /commit/iteminfo/tmall.shop.2.item.2015-10-27.iteminfo.2015-11-01

hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp

spark-submit  --total-executor-cores  120   --executor-memory  10g  --driver-memory 10g opt.py  -inc /commit/iteminfo/20151213/*  20151212 20151213

sh 1_item_inc.sql 20151213