#!/bin/bash
while getopts "s:e:k:c:" arg #选项后面的冒号表示该选项需要参数
do
  case $arg in
       e)
         end_date=$OPTARG
         ;;
       s)
         start_date=$OPTARG
         ;;
       k)
         keywords=$OPTARG
         ;;
       c)
         counts=$OPTARG
         ;;
       ?)
         echo "unknown argument"
         exit 1
         ;;
  esac
done

date_time=$(date -d '0 days ago' +%Y%m%d)
#电商购买记录
t_ec_record=wl_base.t_base_ec_record_dev_new
#闲鱼根据id得到gender
t_userprofile=wlbase_dev.t_base_user_profile_telindex

#id_gender_keyword
out_table_igk=wlservice.t_lel_ec_xinyan_meiya_$date_time""_cd_igk
#id_gender_keywords_buytimes
out_table_igkc=wlservice.t_lel_ec_xinyan_meiya_$date_time""_cd_igkc

echo "-------------------------"
echo "起始时间:"$start_date
echo "结束时间:"$end_date
echo "关键字:""'$keywords'"
echo "购买次数:"$counts
echo "-------------------------"

if [ $# -eq 6 ];then
hive -S -e"
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
create table if not exists $out_table_igk as
select t2.id as id,t1.gender as gender,concat_ws(',',collect_set(t2.keyword)) as keyword from
(select tb_id,xianyu_gender as gender from $t_userprofile where  tb_location regexp '成都') t1
join
(select user_id as id,regexp_extract(title,'$keywords',0) as keyword from $t_ec_record
where (cast(dsn as int) between  $start_date and $end_date) and  title regexp '$keywords') t2
on t1.tb_id = t2.id group by id,gender;"
elif [ $# -eq 8 ];then
hive -S -e"
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
create table if not exists $out_table_igkc as
select t2.id as id,t1.gender as gender,collect_set(t2.keyword) as keyword,size(collect_list(cast(t1.buy_times as int))) as buy_times from
(select tb_id,xianyu_gender as gender from $t_userprofile where tb_location regexp '成都') t1
join
(select user_id as id,regexp_extract(title,'$keywords',0) as keyword from wl_base.t_base_ec_record_dev_new where (cast(dsn as int) between $start_date and $end_date) and title regexp '$keywords') t2
on t1.tb_id = t2.id
where buy_times >= $counts group by id,gender;"
else
echo "no suitable config to executed hive job"
fi
