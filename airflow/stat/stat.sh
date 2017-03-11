#!/bin/bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
path=/home/lel/sparkdata/airflow/stat
echo $today >> ~/stat/stat.log
bash $path/xianyu_iteminfo_stat.sh >> ~/stat/stat.log
bash $path/shopitem_b_stat.sh >> ~/stat/stat.log
bash $path/shopitem_c_stat.sh >> ~/stat/stat.log
bash $path/itemsold_stat.sh >> ~/stat/stat.log
bash $path/item_daysale_stat.sh >> ~/stat/stat.log
bash $path/xianyu_itemcomment_stat.sh >> ~/stat/stat.log
bash $path/iteminfo_stat.sh >> ~/stat/stat.log
bash $path/shopinfo_stat.sh >> ~/stat/stat.log