t_wrt_tmp_menjin_tmall
t_wrt_tmp_menjin_tmall2

create table t_wrt_xifu_menjin_itemid as
select
item_id,
brand_name,
case when paramap['衣门襟'] is null then
	case when paramap['门襟/纽扣'] is null then
		"else"
	else
		paramap['门襟/纽扣']
  end
else
	paramap['衣门襟']
end as type
from t_wrt_xifu_tmall_sale
group by
item_id,
brand_name,
case when paramap['衣门襟'] is null then
	case when paramap['门襟/纽扣'] is null then
		"else"
	else
		paramap['门襟/纽扣']
  end
else
	paramap['衣门襟']
end;

create table t_wrt_tmp_xifu_iteminfo as
select item_id,max(paramap['货号']) as huohao,max(brand_name) as brand_name  from t_wrt_xifu_tmall_sale where paramap['货号'] is not null group by item_id

