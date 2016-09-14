

-- tb_id	string	淘宝id
-- alipay	string	支付宝实名验证
-- buycnt	string	用户从注册到16年5月购买商品总数
-- verify	string	用户淘宝等级 0-6
-- regtime	string	用户淘宝注册时间
-- tb_nick	string	淘宝昵称
-- tb_location	string	淘宝地址
-- qq_gender	string	qq 性别
-- qq_age	int	qq年龄
-- qq_name	string	qq 姓名
-- qq_loc	string	qq 地址
-- tel_prov	string	手机号 对应地址省份
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

create table t_base_user_profile as

SELECT

case when tb_id                 is null or  length(tb_id                )<1  then '-'  else  tb_id                  end as tb_id                  ,
case when alipay                is null or  length(alipay               )<1  then '-'  else  alipay                 end as alipay                 ,
case when buycnt                is null or  length(buycnt               )<1  then '-'  else  buycnt                 end as buycnt                 ,
case when verify                is null or  length(verify               )<1  then '-'  else  verify                 end as verify                 ,
case when regtime               is null or  length(regtime              )<1  then '-'  else  regtime                end as regtime                ,
case when tb_nick               is null or  length(tb_nick              )<1  then '-'  else  tb_nick                end as tb_nick                ,
case when tb_location           is null or  length(tb_location          )<1  then '-'  else  tb_location            end as tb_location            ,
case when qq_gender             is null or  length(qq_gender            )<1  then '-'  else  qq_gender              end as qq_gender              ,
case when qq_age                is null                   then -1  else  qq_age                 end as qq_age                 ,
case when qq_name               is null or  length(qq_name              )<1  then '-'  else  qq_name                end as qq_name                ,
case when qq_loc                is null or  length(qq_loc               )<1  then '-'  else  qq_loc                 end as qq_loc                 ,
case when tel_prov              is null or  length(tel_prov             )<1  then '-'  else  tel_prov               end as tel_prov               ,
case when tel_city              is null or  length(tel_city             )<1  then '-'  else  tel_city               end as tel_city               ,
case when xianyu_gender         is null or  length(xianyu_gender        )<1  then '-'  else  xianyu_gender          end as xianyu_gender          ,
case when xianyu_birthday       is null 										then -1  else  xianyu_birthday        end as xianyu_birthday        ,
case when xianyu_constellation  is null or  length(xianyu_constellation )<1  then '-'  else  xianyu_constellation   end as xianyu_constellation   ,
case when xianyu_province       is null or  length(xianyu_province      )<1  then '-'  else  xianyu_province        end as xianyu_province        ,
case when xianyu_city           is null or  length(xianyu_city          )<1  then '-'  else  xianyu_city            end as xianyu_city            ,
case when predict_gender        is null 								     then -1  else  predict_gender         end as predict_gender         ,
case when xianyu_detail_loc     is null or  length(xianyu_detail_loc    )<1  then '-'  else  xianyu_detail_loc      end as xianyu_detail_loc      ,
case when weibo_id              is null or  length(weibo_id             )<1  then '-'  else  weibo_id               end as weibo_id               ,
case when weibo_screen_name     is null or  length(weibo_screen_name    )<1  then '-'  else  weibo_screen_name      end as weibo_screen_name      ,
case when weibo_gender          is null or  length(weibo_gender         )<1  then '-'  else  weibo_gender           end as weibo_gender           ,
case when weibo_followers_count is null 									then -1  else  weibo_followers_count  end as weibo_followers_count  ,
case when weibo_friends_count   is null	  									then -1  else  weibo_friends_count    end as weibo_friends_count    ,
case when weibo_statuses_count  is null 		 							then -1  else  weibo_statuses_count   end as weibo_statuses_count   ,
case when weibo_created_at      is null or  length(weibo_created_at     )<1  then '-'  else  weibo_created_at       end as weibo_created_at       ,
case when weibo_location        is null or  length(weibo_location       )<1  then '-'  else  weibo_location         end as weibo_location         ,
case when weibo_verified        is null or  length(weibo_verified       )<1  then '-'  else  weibo_verified         end as weibo_verified         ,
case when weibo_colleges        is null or  length(weibo_colleges       )<1  then '-'  else  weibo_colleges         end as weibo_colleges         ,
case when weibo_company  	    is null or  length(weibo_company       )<1  then '-'  else  weibo_company         end as weibo_company
from
t_base_user_info_s_tbuserinfo_t_step7 ;
