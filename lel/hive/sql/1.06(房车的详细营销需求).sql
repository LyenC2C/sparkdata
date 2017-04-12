
1,地区：杭州；
2,数量：200；
3,关注微博： 
http://weibo.com/zoeco
http://weibo.com/gwrv
http://weibo.com/selfdriving
http://weibo.com/BritzCampervan
http://weibo.com/aoteduo

1960399214
2060352104
1808077622
3280900903
3946144634
4,输出字段：用户号码,用户微博标签
	
t_base_weibo_user_fri
t_base_weibo_user_new


create table if not exists wl_service.t_lel_weibo_fangche_20170106 as
select t3.id as id,t4.tags as tags from 
(select t1.id as id
from
(select id
from t_base_weibo_user_fri
where ds =20161106
and ids regexp '1960399214|2060352104|1808077622|3280900903|3946144634'
) t1
join
(select id
from
t_base_weibo_user_new
where ds = 20161123
and location regexp '杭州'
) t2
on t1.id = t2.id
) t3
join 
(select id,tags from t_base_weibo_usertag where ds = 20161115) t4
on t3.id=t4.id




cs105:/hive/warehouse/wl_service.db/t_lel_weibo_fangche_20170106 
微博id换电话
放在cs105:/user/lel/weibo
#awk -F '\001' 'NR==FNR{a[$1]=$2}a[$2]{print $1"\t"a[$2]}' fangche20170106_hive t_lel_weibo_fangche_20170106.telsnwbname , sort , uniq > 20170106_result_fangche
awk -F '\001' 'NR==FNR{a[$1]=$2}{if(($2 in a))print $1"\t"a[$2]}' fangche20170106_hive t_lel_weibo_fangche_20170106.telsnwbname , sort , uniq > 20170106_result_fangche



