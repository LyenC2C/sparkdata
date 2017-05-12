daikuan_20170420_weiboand58



weibo:
drop table t_lel_weibo_daikuan_20170420_res;
create table t_lel_weibo_daikuan_20170420_res
as
select distinct b.phone,a.description
from
(select * from t_lel_weibo_daikuan_20170420)a
join
(select * from t_lel_weibo_daikuan_phone20170420)b
on cast(a.id as string) = b.wbid

58:
drop table wl_service.t_lel_58_daikuai_20170420;
create table wl_service.t_lel_58_daikuai_20170420
as
select  max(a.catename)as catename,a.decrypted_tel as cell,max(a.nickname) as nickname,max(a.title)as title,max(a.uname)as uname,max(b.cate)as cate
from
(select  catename,decrypted_tel,nickname,title,uname from wl_base.t_base_credit_58_info)a
join
(select decrypted_tel,cate from wl_base.t_base_credit_58_info_fraud_1208filtered where cate = '贷款')b
on a.decrypted_tel=b.decrypted_tel
group by a.decrypted_tel