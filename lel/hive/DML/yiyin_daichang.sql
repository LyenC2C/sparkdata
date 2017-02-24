LOAD DATA  INPATH '/user/lel/temp/yixin_daichang' OVERWRITE INTO TABLE wl_base.t_base_yixin_daichang PARTITION (ds='0000tmp');

insert into table wl_base.t_base_yixin_daichang partition(ds='20170223')
select phone,platform,case when concat_ws(',',collect_list(flag)) regexp 'True' then 'True' else 'False' end as flag
from
(select phone,platform,flag from  wl_base.t_base_yixin_daichang where ds = '0000tmp'
union all
select phone,platform,flag from wl_base.t_base_yixin_daichang where ds = '20170220')t
group by phone,platform
