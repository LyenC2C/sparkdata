
-- 加入闲鱼地址信息

Drop table  t_base_user_info_s_tbuserinfo_t_step5 ;

create table t_base_user_info_s_tbuserinfo_t_step5 as
SELECT
t1.* ,
t2.xianyu_detail_loc

FROM
(
select userid ,concat_ws('|',collect_set(loc)) as xianyu_detail_loc   FROM
(
select userid,concat_ws('_',province,city,area,community_name) as loc from t_base_ec_tb_xianyu_item
where province <> '' and province <> '-' and city <> ''and city <> '-' and area <> '' and area <> '-'

group by userid,province,city,area,community_name
)t group by userid

) t2
 RIGHT join
 t_base_user_info_s_tbuserinfo_t_step4 t1
on t1.tb_id=t2.userid
;


