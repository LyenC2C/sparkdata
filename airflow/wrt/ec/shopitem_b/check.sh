#!/bin/bash
source ~/.bashrc
date=$1
hadoop fs -test -e /commit/shopitem_b/*$date*
if [ $? -eq 0 ] ;then
        size=`hadoop fs -count -q /commit/shopitem_b/*$date* | awk '{print $7}'`
        if [ $size -gt 0 ];then
        #目录存在且容量大于0
        echo  '1'
        else
        #目录存在且容量小于0
        echo  '0'
        fi
else
    #目录不存在
    echo  '0'
fi
