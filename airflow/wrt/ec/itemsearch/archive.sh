#!/usr/bin/env bash
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)
hadoop fs -mkdir /commit/itemsearch/archive/$today'_arc'
hadoop fs -mv /commit/itemsearch/20*/* /commit/itemsearch/archive/$today'_arc'/
