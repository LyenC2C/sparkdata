
create table wlcredit.t_credit_xianyu_zhima_userinfo as
select
tel,userid ,verify
from
  (
    SELECT
        tel_index,
      uid as tel,
      tb_id,
      verify
      from
    (
    SELECT
      tel_index,
      tb_id,
      verify
    FROM wlbase_dev.t_base_user_profile_telindex
    WHERE tel_index <> '-' AND verify <> '-'
    GROUP BY tel_index, tb_id, verify
  )t  join wlrefer.t_zlj_phone_rank_index tn  on t.tel_index=tn.tel_index
) t1
right join
(
  select userid,zhima
from
wlbase_dev.t_base_ec_xianyu_iteminfo
where ds='20161218' and zhima='1'
 group by userid,zhima
)t2 on t1.tb_id=t2.userid
;


drop table wlcredit.t_credit_xianyu_zhima_userinfo_filter7w ;
create table wlcredit.t_credit_xianyu_zhima_userinfo_filter7w as
  select
t1.userid, t2.uid
    from
  (select
  t1.userid ,t1.tel_index
from
wlcredit.t_credit_xianyu_zhima_userinfo
t1 left join wlfinance.t_zlj_base_match t2 on t2.ds='7wid' and t1.tel_index=t2.tel
where t2.tel is null
  )t1
      left join
  wlrefer.t_zlj_phone_rank_index t2 on t1.tel_index =t2.tel_index
  order by t2.uid
;