#!/usr/bin/env bash

pre_path='/home/wrt/sparkdata/wrt'

today=$(date -d '0 days ago' +%Y%m%d)
lastday=$(date -d '7 days ago' +%Y%m%d)

sh $pre_path/data_base_process/sh/item_shop.sh $today $lastday &> \
$pre_path/data_base_process/sh/log_itemshop/log_$today