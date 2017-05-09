load data inpath "/user/lel/temp/multiplatform_kakadai" overwrite into table wl_base.t_base_multiplatform partition(ds='00tmp')
insert into table wl_base.t_base_multiplatform partition(ds='20170417')
select phone,platform,case when concat_ws(',',collect_list(flag)) regexp 'True' then 'True' else 'False' end as flag
from
(select phone,platform,flag from  wl_base.t_base_multiplatform where ds = '00tmp'
union all
select phone,platform,flag from wl_base.t_base_multiplatform where ds = '20170401')t
group by phone,platform
