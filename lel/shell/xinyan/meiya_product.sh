#!/bin/bash

while getopts "c:l:" arg #选项后面的冒号表示该选项需要参数
do
  case $arg in
       c)
         count=$OPTARG
         ;;
       l)
         last_teeth_tel=$OPTARG
         ;;
       ?)
         echo "unknown argument"
         exit 1
         ;;
  esac
done

last_tel_date_time=teeth_lel_tel_$last_teeth_tel
date_time=$(date -d '1 days ago' +%Y%m%d)
path=/home/lel/work/

if [ $# -eq 4 ];then

if [ ! -d "$path/xinyan_yamei/$date_time" ]; then
    mkdir $path/xinyan_yamei/""$date_time
fi

cd $path/xinyan_yamei/""$date_time
hadoop fs -test -e /trans/lel/t_lel_ec_xinyan_$date_time""_cd_igk.teltbname
if [ $? -eq 0 ] ;then
    #attach phone number
    hadoop fs -test -e /hive/warehouse/wlservice.db/t_lel_ec_xinyan_$date_time""_cd_igk
    if [ $? -eq 0 ] ;then
    hadoop fs -cat /hive/warehouse/wlservice.db/t_lel_ec_xinyan_$date_time""_cd_igk/*  > $date_time""_yamei
    hadoop fs -get /trans/lel/t_lel_ec_xinyan_$date_time""_cd_igk.teltbname ./
    awk -F '\001' 'NR==FNR && $3!="None"{a[$2]=$1"\t"$3}NR!=FNR{if(($1 in a))print a[$1]"\t"$2"\t"$3}' t_lel_ec_xinyan_$date_time""_cd_igk.teltbname $date_time""_yamei > $date_time
    #compare to last time
    awk -F '\001' 'NR==FNR{a[$1]=$1}NR!=FNR{if(!($1 in a))print $0}' ../teeth_tel/""$last_tel_date_time $date_time > $date_time""_distincted
    counts=`wc -l $date_time""_distincted | awk '{print $1}'`
    if [ $counts -gt $count ];then
    head -n$count $date_time""_distincted > $date_time""_distincted_$count
    phone_num=`awk -F '\t' '{print $1}' $date_time""_distincted_$count`
    cp ../teeth_tel/""$last_tel_date_time ../teeth_tel/teeth_lel_tel_$date_time
    $phone_num >> ../teeth_tel/teeth_lel_tel_$date_time
    else
    echo "the result number is not enough,use $date_time""_distincted instead"
    fi
    else
    echo "Sorry,the file(/hive/warehouse/wlservice.db/t_lel_ec_xinyan_$date_time""_cd_igk) required do not exist at this moment!!!"
    fi
else
    echo "Sorry,the file(/trans/lel/t_lel_ec_xinyan_$date_time""_cd_igk.teltbname) required do not exist at this moment!!!"
fi
else
echo "less params!!!"
exit 1
fi

