pre_path='/home/wrt/sparkdata'

hadoop fs -rm -r /user/wrt/sale_tmp
spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151222 20151221
sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151222

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc  \
#/commit/iteminfo/20151225/*  20151224 20151225
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151223/*  20151222  20151223
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151223

#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151224/*  20151223  20151224
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151224
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151225/*  20151224  20151225
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151225

#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151223 20151222
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151223

#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151224 20151223
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151224
#
#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151225 20151224
#sh $pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151225


#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151216 20151217 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151217 20151218 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151218 20151219 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151219 20151220 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151220 20151221 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151221 20151222 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151222 20151223 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151223 20151224 20151224 20151224
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151224 20151225 20151224 20151224



#hadoop fs -rm -r /user/wrt/sale_tmp
#spark-submit  --executor-memory 12G  --driver-memory 20G  --total-executor-cores 120 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151222 20151221
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.sql 20151222


#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151223/*  20151222 20151223
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151224/*  20151223 20151224

#sh $pre_path/zlj/project/base_data_process/hive/item/step.sh 20151219  20151220
#sh $pre_path/zlj/project/base_data_process/hive/item/step.sh 20151220  20151221
#sh $pre_path/zlj/project/base_data_process/hive/item/step.sh 20151221  20151222

#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151220/*  20151219  20151220
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151220
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151221/*  20151220  20151221
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151221
#
#hadoop fs -rm -r /user/zlj/data/temp/t_base_ec_item_dev_tmp
#spark-submit  --total-executor-cores  120   --executor-memory  12g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc_opt.py  -inc /commit/iteminfo/20151222/*  20151221  20151222
#sh $pre_path/zlj/project/base_data_process/hive/item/1_item_inc.sql 20151222

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151218 20151217
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151219 20151218
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151220 20151219

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151221 20151220
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151222 20151221
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151223 20151222

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151216 20151217 20151212 20151212


#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151218/*  20151217 20151218

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151217/*  20151216 20151217
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151219/*  20151217 20151219
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151220/*  20151219 20151220
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151221/*  20151220 20151221
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151222/*  20151221 20151222




#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151218 20151217
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151219 20151218
#
#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151220 20151219


#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151212 20151211
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151213 20151212
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151214 20151213
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151215 20151214
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151216 20151215
#
#spark-submit  --executor-memory 20G  --driver-memory 10G  --total-executor-cores 200 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151217 20151216
#
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151209 20151210 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151210 20151211 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151211 20151212 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151212 20151213 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151213 20151214 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151214 20151215 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151215 20151216 20151212 20151212
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151216 20151217 20151212 20151212

#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151027
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151028
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151029
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151030
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151102
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151104
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151105
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151106
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151107
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151108
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151109
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151110
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151111
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151112
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151113
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151114
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151115
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151116
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151117
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151118
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151119
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151120
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151121
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151122
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151123
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151124
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151125
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151126
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151127
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151128
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151129
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151130
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151201
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151202
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151203
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151204
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151205
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151206
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151207
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151208
#spark-submit  --total-executor-cores  120  --executor-memory 10g  --driver-memory 16g $pre_path/wrt/data_base_process/ 20151209

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/wrt/data_base_process/ 20151210

#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151206/*  20151205 20151206
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151207/*  20151206 20151207
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151208/*  20151207 20151208
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151209/*  20151208 20151209
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151210/*  20151209 20151210
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151211/*  20151210 20151211

#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151212/*  20151211 20151212
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151213/*  20151212 20151213
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151214/*  20151213 20151214
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151215/*  20151214 20151215

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151204 20151203
#
#spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151204/*  20151203 20151204
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 10g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151204/*  20151203 20151204

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151130 20151201 20151202 20151202
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151201 20151202 20151202 20151202

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151201/*  20151130 20151201
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151202/*  20151201 20151202
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151201/*  20151130 20151201
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151202/*  20151201 20151202

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151119 20151120 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151120 20151121 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151121 20151122 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151122 20151123 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151123 20151124 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151124 20151125 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151125 20151126 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151126 20151127 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151127 20151128 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151128 20151129 20151130 20151130
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151129 20151130 20151130 20151130

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151126/*  20151125 20151126
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151127/*  20151126 20151127
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151128/*  20151127 20151128
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151129/*  20151128 20151129
#
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151130/*  20151129 20151130

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151126/*  20151125 20151126
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151127/*  20151126 20151127

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151128/*  20151127 20151128
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151129/*  20151128 20151129
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151130/*  20151129 20151130

#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151125 20151124
#
#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151126 20151125
#
#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151127 20151126
#
#spark-submit  --executor-memory 8G  --driver-memory 8G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151128 20151127

#spark-submit  --executor-memory 8G  --driver-memory 10G  --total-executor-cores 80 \
#$pre_path/wrt/data_base_process/t_wrt_base_ec_item_sale.py 20151130 20151129

#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151118/*  20151117 20151118
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151119/*  20151118 20151119
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151120/*  20151119 20151120
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151121/*  20151120 20151121
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151122/*  20151121 20151122
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151123/*  20151122 20151123
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151124/*  20151123 20151124
#
#spark-submit  --total-executor-cores 80 --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
#/commit/iteminfo/20151125/*  20151124 20151125

#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151121/*  20151120 20151121
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151122/*  20151121 20151122
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151123/*  20151122 20151123
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151124/*  20151123 20151124
#spark-submit  --total-executor-cores  80  --executor-memory 8g  --driver-memory 8g \
#$pre_path/zlj/project/base_data_process/hive/shop/1_shop_inc.py  -inc \
#/commit/iteminfo/20151125/*  20151124 20151125

#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151029 20151028
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151030 20151029
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151102 20151030
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151104 20151102
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151105 20151104
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151106 20151105
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151107 20151106
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151108 20151107
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151109 20151108
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151110 20151109
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151111 20151110
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151112 20151111
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151113 20151112
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151114 20151113
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151115 20151114
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151116 20151115
#spark-submit  --executor-memory 4G  --driver-memory 4G  --total-executor-cores 20 \
#$pre_path/wrt/data_base_process/total_sale_tianbu.py 20151117 20151116

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151026 20151027 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151027 20151028 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151028 20151029 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151029 20151030 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151030 20151102 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151102 20151104 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151104 20151105 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151105 20151106 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151106 20151107 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151107 20151108 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151108 20151109 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151109 20151110 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151110 20151111 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151111 20151112 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151112 20151113 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151113 20151114 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151114 20151115 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151115 20151116 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql  20151116 20151117 20151112 20151112

#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151111
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151112
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151113
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151114
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151115
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151116
#sh $pre_path/wrt/data_base_process/hive/t_wrt_quchong.sql 20151117

#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151026 20151027 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151027 20151028 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151028 20151029 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151029 20151030 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151030 20151102 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151102 20151104 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151104 20151105 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151105 20151106 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151106 20151107 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151107 20151108 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151108 20151109 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151109 20151110 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151110 20151111 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151111 20151112 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151112 20151113 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151113 20151115 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151115 20151116 20151112 20151112
#sh $pre_path/zlj/project/task/zhejiang/everyday_sold.sql   20151116 20151117 20151112 20151112
