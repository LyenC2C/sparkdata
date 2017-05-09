--统计队列示例：
source ~/.bashrc

today=$(date -d '0 days ago' +%Y%m%d)

insert overwrite  table t_lel_record_backflow_multi_fields_standard PARTITION(ds = $today)
select * from
(
select *,"贷款类" as cate from
(select phone,idbank,idcard,name,max(to_date(`date`)) as latest, count(1)as times from wl_service.t_lel_record_data_backflow_combined  where cate = '贷款类' group by phone,idbank,idcard,name)a
union all
select *,"征信类" as cate from
(select phone,idbank,idcard,name,max(to_date(`date`)) as latest, count(1)as times from wl_service.t_lel_record_data_backflow_combined  where cate = '征信类' group by phone,idbank,idcard,name)b
)c

