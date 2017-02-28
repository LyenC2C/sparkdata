#!/bin/bash
today=$(date -d '0 days ago' +%Y%m%d)
hadoop fs -mkdir /commit/iteminfo/archive/$today'_arc'
hadoop fs -mv /commit/iteminfo/20*/* /commit/iteminfo/archive/$today'_arc'/
