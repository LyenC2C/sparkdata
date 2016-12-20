

1男 2女

-- tb_id	string	淘宝id
-- alipay	string	支付宝实名验证
-- buycnt	string	用户从注册到16年5月购买商品总数
-- verify	string	用户淘宝等级 0-6
-- regtime	string	用户淘宝注册时间
-- tb_nick	string	淘宝昵称
-- tb_location	string	淘宝地址
-- qq_gender	string	qq性别
-- qq_age	int	qq年龄
-- qq_name	string	qq姓名
-- qq_loc	string	qq地址
-- tel_prov	string	手机号对应地址省份
-- tel_city	string	手机号对应地址城市
-- xianyu_gender	string	闲鱼性别
-- xianyu_birthday	int	闲鱼年龄（出生年月转换而来）
-- xianyu_constellation	string	闲鱼星座（出生年月转换而来）
-- xianyu_province	string	闲鱼省份
-- xianyu_city	string	闲鱼城市
-- predict_gender	int	模型预测年龄覆盖所有够买用户
-- xianyu_detail_loc	string	闲鱼详细地址（通过GPS坐标转换）
-- weibo_id	bigint	微博 ID
-- weibo_screen_name	string	微博姓名
-- weibo_gender	string	微博性别
-- weibo_followers_count	int	微博关注数
-- weibo_friends_count	int	微博被关注数
-- weibo_statuses_count	int	微博总条数
-- weibo_created_at	string	微博注册时间
-- weibo_location	string	微博地址
-- weibo_verified	string	是否大v认证
-- weibo_colleges	string	微博教育信息
-- weibo_company	string	微博职位信息

--用户基础信息表

Drop table  t_base_user_profile;
create table t_base_user_profile as
SELECT
   tb_id                 ,alipay                ,buycnt
    ,verify                ,regtime               ,tb_nick
    ,tb_location           ,qq_gender             ,qq_age
    ,qq_name               ,qq_loc                ,qq_find_schools
    ,tel_loc               ,xianyu_gender         ,xianyu_birthday
    ,xianyu_constellation  ,xianyu_province       ,xianyu_city           ,xianyu_detail_loc
    ,model_predict_gender  ,weibo_id              ,weibo_screen_name     ,weibo_gender
    ,weibo_followers_count ,weibo_friends_count   ,weibo_statuses_count  ,weibo_created_at
    ,weibo_location        ,weibo_verified        ,weibo_colleges        ,weibo_company
    ,58_tel                ,58_nickname
from
  (SELECT
case when tb_id                 is null or  length(tb_id                )<1  then NULl   else  tb_id                  end as tb_id                  ,
case when alipay                is null or  length(alipay               )<=1  then NULl   else  alipay                 end as alipay                 ,
case when buycnt                is null or  length(buycnt               )<=1  then NULl   else  buycnt                 end as buycnt                 ,
case when verify                is null or  length(verify               )<=1  then NULl   else  verify                 end as verify                 ,
case when regtime               is null or  length(regtime              )<=1  then NULl   else  regtime                end as regtime                ,
case when tb_nick               is null or  length(tb_nick              )<=1  then NULl   else  tb_nick                end as tb_nick                ,
case when tb_location           is null or  tb_location='-' then NULL  when length(tb_location          )<=1  then NULl   else  tb_location            end as tb_location            ,
case when qq_gender             is null or  length(qq_gender            )<=1  then NULl   else  cast(qq_gender  as int)             end as qq_gender              ,
case when qq_age                is null                   then NULL   else  cast(qq_age as int)                 end as qq_age                 ,
case when qq_name               is null or  length(qq_name              )<=1  then NULl   else  qq_name                end as qq_name                ,
case when qq_loc                is null or  length(qq_loc               )<=1  then NULl   else  qq_loc                 end as qq_loc                 ,
case when qq_find_schools                is null or  length(qq_loc      )<=1  then NULl   else  qq_find_schools        end as qq_find_schools        ,
case when tel_prov              is null or  length(tel_prov             )<=1  then NULl   else  concat_ws(' ',tel_prov ,tel_city)               end as tel_loc              ,
case when xianyu_gender         is null or  length(xianyu_gender        )<=1  then NULl   else  xianyu_gender          end as xianyu_gender          ,
case when xianyu_birthday       is null 										then NULL  else  xianyu_birthday        end as xianyu_birthday        ,
case when xianyu_constellation  is null or  length(xianyu_constellation )<=1  then NULl   else  xianyu_constellation   end as xianyu_constellation   ,
case when xianyu_province       is null or  length(xianyu_province      )<=1  then NULl   else  xianyu_province        end as xianyu_province        ,
case when xianyu_city           is null or  length(xianyu_city          )<=1  then NULl   else  xianyu_city            end as xianyu_city            ,
case when xianyu_detail_loc     is null or  length(xianyu_detail_loc    )<=1  then NULl   else  xianyu_detail_loc      end as xianyu_detail_loc      ,
case when predict_gender        is null 			 					     then NULL   else  predict_gender         end as model_predict_gender         ,
case when weibo_id              is null or  length(weibo_id             )<=1  then NULl   else  weibo_id               end as weibo_id               ,
case when weibo_screen_name     is null or  length(weibo_screen_name    )<=1  then NULl   else  weibo_screen_name      end as weibo_screen_name      ,
case when weibo_gender          is null or  length(weibo_gender         )<=1  then NULl   else  weibo_gender           end as weibo_gender           ,
case when weibo_followers_count is null 									then NULL  else  weibo_followers_count  end as weibo_followers_count  ,
case when weibo_friends_count   is null	  									then NULL  else  weibo_friends_count    end as weibo_friends_count    ,
case when weibo_statuses_count  is null 		 							then NULL  else  weibo_statuses_count   end as weibo_statuses_count   ,
case when weibo_created_at      is null or  length(weibo_created_at     )<=1  then NULl   else  weibo_created_at       end as weibo_created_at       ,
case when weibo_location        is null or  length(weibo_location       )<=1  then NULl   else  weibo_location         end as weibo_location         ,
case when weibo_verified        is null or  length(weibo_verified       )<=1  then NULl   else  weibo_verified         end as weibo_verified         ,
case when weibo_colleges        is null or  length(weibo_colleges       )<=1  then NULl   else  weibo_colleges         end as weibo_colleges         ,
case when weibo_company  	    is null or  length(weibo_company       )<=1  then NULl   else  weibo_company         end as weibo_company ,
case when   58_tel  	    is null or  length(58_tel       )<=1  then NULl   else  58_tel         end as 58_tel ,
case when 58_nickname  	    is null or  length(58_nickname       )<=1  then NULl   else  58_nickname         end as 58_nickname
from
t_base_user_info_s_tbuserinfo_t_step9
)t
  group by
    tb_id                 ,alipay                ,buycnt
    ,verify                ,regtime               ,tb_nick
    ,tb_location           ,qq_gender             ,qq_age
    ,qq_name               ,qq_loc                ,qq_find_schools
    ,tel_loc               ,xianyu_gender         ,xianyu_birthday
    ,xianyu_constellation  ,xianyu_province       ,xianyu_city           ,xianyu_detail_loc
    ,model_predict_gender  ,weibo_id              ,weibo_screen_name     ,weibo_gender
    ,weibo_followers_count ,weibo_friends_count   ,weibo_statuses_count  ,weibo_created_at
    ,weibo_location        ,weibo_verified        ,weibo_colleges        ,weibo_company
    ,58_tel                ,58_nickname
;




-- test

SELECT *
where tb_id = '1055265006' or tb_id = '1622665460' ;




-- select sum( case when  weibo_verified='True' then 1 else 0 endy )
-- from t_base_user_profile  ;
--
-- 523696596
-- SELECT count(1) from
--   (select tb_id from t_base_user_profile GROUP BY  tb_id)t ;
--
-- 525891768
-- select count(1) from t_base_user_profile  ;
--
--
-- --2000w去重
-- create table t_base_user_profile_disinct as
-- SELECT
--   tb_id                 ,
-- alipay                ,
-- buycnt                ,
-- verify                ,
-- regtime               ,
-- tb_nick               ,
-- tb_location           ,
-- qq_gender             ,
-- qq_age                ,
-- qq_name               ,
-- qq_loc                ,
-- qq_find_schools       ,
-- tel_loc               ,
-- xianyu_gender         ,
-- xianyu_birthday       ,
-- xianyu_constellation  ,
-- xianyu_province       ,
-- xianyu_city           ,
-- xianyu_detail_loc     ,
-- model_predict_gender  ,
-- weibo_id              ,
-- weibo_screen_name     ,
-- weibo_gender          ,
-- weibo_followers_count ,
-- weibo_friends_count   ,
-- weibo_statuses_count  ,
-- weibo_created_at      ,
-- weibo_location        ,
-- weibo_verified        ,
-- weibo_colleges        ,
-- weibo_company         ,
-- 58_tel                ,
-- 58_nickname
--   from
-- (
-- select *, row_number() over(distribute by tb_id sort by buycnt) as rn
--
-- from t_base_user_profile
-- )t where rn=1;
--
--

