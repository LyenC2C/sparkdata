nums:31299
广发发卡用户行为分析需求: 2297209690

1.有多少同时是我们库中的他行信用卡客户
total: 	
select count(1)
from
(select phone from t_base_guangfa_credit_customer) t1
join
(select distinct(phone) from t_base_credit_bank where ds=20170222 and flag='True')t2
on t1.phone = t2.phone
jiaotong:5508
xingye:935
intersection:335
select count(1)
from
(select phone from t_base_guangfa_credit_customer) t1
join
(select phone,collect_list(platform) as p from t_base_credit_bank where ds=20170222 and flag='True' group by phone having size(p)=2 )t2
on t1.phone = t2.phone


2.有多少在信用卡代偿平台中注册        11
select count(1)
from
(select phone from t_base_guangfa_credit_customer) t1
join
(select distinct(phone) from t_base_yixin_daichang where ds=20170222 and flag='True')t2
on t1.phone = t2.phone

3.有多少微博关注除广发外的信用卡大V    2122775
create table wlservice.t_lel_except_guangfa_verified_weiboid
as
select fri.id
from
(select id from t_base_weibo_user_fri where ds=20161106  and ids not regexp '2297209690') fri
join 
(select id 
from t_base_weibo_user_new 
where ds=20161123  and verified='True') new
on fri.id=new.id

4.有多少用户有网购消费记录 31299->13150->10452


