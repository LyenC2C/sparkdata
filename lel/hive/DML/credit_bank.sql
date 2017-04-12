LOAD DATA  INPATH '/user/lel/temp/credit_bank' OVERWRITE INTO TABLE wl_base.t_base_credit_bank PARTITION (ds='00tmp');

insert into table wl_base.t_base_credit_bank partition(ds='20170222')
select phone,platform,case when concat_ws(',',collect_list(flag)) regexp 'True' then 'True' else 'False' end as flag
from
(select phone,platform,flag from  wl_base.t_base_credit_bank where ds = '00tmp'
union all
select phone,platform,flag from wl_base.t_base_credit_bank where ds = '20170221')t
group by phone,platform


