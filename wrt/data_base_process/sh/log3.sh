#!/usr/bin/env bash
pre_path='/home/wrt/sparkdata'

spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/20151213/*  20151212 20151213

spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/20151214/*  20151213 20151214

spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/20151215/*  20151214 20151215

spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/20151216/*  20151215 20151216

spark-submit  --total-executor-cores  80   --executor-memory  8g  --driver-memory 10g \
$pre_path/zlj/project/base_data_process/hive/item/1_item_inc.py  -inc \
/commit/iteminfo/20151217/*  20151216 20151217

