#!/usr/bin/env bash
source ~/.bashrc

spark-submit --executor-memory 9G  --driver-memory 8G  --total-executor-cores 120\
./repair_shopitem_b_sold.py 20160908 20160907
