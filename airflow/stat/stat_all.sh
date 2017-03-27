#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
path=/home/lel/sparkdata/airflow/stat

echo $today >> ~/stat/stat2.log

bash $path/static_stat_impala.sh wl_base t_base_ec_xianyu_iteminfo >> ~/stat/stat2.log

bash $path/static_stat_impala.shwl_base t_base_ec_shopitem_b >> ~/stat/stat2.log

bash $path/static_stat_impala.sh wl_base t_base_ec_shopitem_c >> ~/stat/stat2.log

bash $path/static_stat_impala.sh wl_base t_base_ec_item_sold_dev >> ~/stat/stat2.log

bash $path/static_stat_impala.sh wl_base t_base_ec_item_daysale_dev_new >> ~/stat/stat2.log

bash $path/static_stat_impala.sh wl_base t_base_ec_xianyu_itemcomment >> ~/stat/stat2.log

bash $path/static_stat_hive.sh wl_base t_base_ec_item_dev_new >> ~/stat/stat2.log

bash $path/static_stat_impala.sh wl_base t_base_ec_shop_dev_new >> ~/stat/stat2.log
