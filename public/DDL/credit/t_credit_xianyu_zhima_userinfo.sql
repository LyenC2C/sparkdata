
create table wlcredit.t_credit_xianyu_zhima_userinfo as
select
tel_index,userid ,verify
from
(
  select
tel_index,tb_id,verify
from wlbase_dev.t_base_user_profile_telindex
where tel_index <> '-' and verify <> '-'
group by tel_index,tb_id,verify
) t1
right join
(
  select userid,zhima
from
wlbase_dev.t_base_ec_xianyu_iteminfo
where ds='20161218' and zhima='1'
 group by userid,zhima
)t2 on t1.tb_id=t2.userid