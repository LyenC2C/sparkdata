


#sh   1_t_zlj_ec_userbuy.sql  商品最新ds  feed_ds
sh   1_t_zlj_ec_userbuy.sql
sh   2_perfer_dim.sql
#sh   2_1_perfer_dim_tag.sql

sh  3_perfer_brand.sql
sh  4_perfer_shop.sql   20160622

sh  5.2_perfer_price.sh

sh 6_perfer_car.sql
sh 7_perfer_house.sql

#sh 8_perfer_freq.sql
#spark-submit  --total-executor-cores  200   --executor-memory  20g  --driver-memory 20g hmm.py 100  5 20150701
sh 9_cat_tags.sh
spark-submit  --total-executor-cores  100   --executor-memory  15g  --driver-memory 15g   merge.py  -merge