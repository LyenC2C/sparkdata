--1.1

select wine_type,count(1)as brand_num
from
(select
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END
as wine_type,
brand_id
from
t_base_ec_item_dev_new
where
ds = 20161013
and
root_cat_id = 50008141
group by 
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END
as wine_type,
brand_id)t
group by
wine_type

--1.2

select count(1) as num from
(select
brand_id
from
t_base_ec_item_dev_new
where
ds = 20161013
and
root_cat_id = 50008141
and
cat_id not in ("50008143","50013004","50013006","123224006","50013006","123214002","50013052","123502002","50008144","50008148","50008147")
group by
brand_id
)t

--1.3

select count(1) as num from
(select
brand_id
from
t_base_ec_item_dev_new
where
ds = 20161013
and
root_cat_id = 50008141
group by
brand_id
)t


--2.1

select
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END
as wine_type,
count(1) as num
from
t_base_ec_item_dev_new
where
ds = 20161013
and
root_cat_id = 50008141
and
is_online = '1'
group by
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END


--2.2
select
count(1) as num
from
t_base_ec_item_dev_new
where
ds = 20161013
and
root_cat_id = 50008141
AND
cat_id not in ("50008143","50013004","50013006","123224006","50013006","123214002","50013052","123502002","50008144","50008148","50008147")
and
is_online = '1'

--2.3

select
count(1) as num
from
t_base_ec_item_dev_new
where
ds = 20161013
and
root_cat_id = 50008141
and
is_online = '1'

--3.1 评论

select
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END
as wine_type,
count(1) as num
from
t_base_ec_record_dev_new_simple
where
ds = 'true'
and
root_cat_id = 50008141
group by
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END


--3.2

select
count(1) as num
from
t_base_ec_record_dev_new_simple
where
ds = 'true'
and
root_cat_id = 50008141
AND
cat_id not in ("50008143","50013004","50013006","123224006","50013006","123214002","50013052","123502002","50008144","50008148","50008147")

--3.3

select
count(1) as num
from
t_base_ec_record_dev_new_simple
where
ds = 'true'
and
root_cat_id = 50008141


--4.1 消费人数
select wine_type,count(1) as num
from
(
select
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END
as wine_type,
user_id
from
t_base_ec_record_dev_new_simple
where
ds = 'true'
and
root_cat_id = 50008141
group by
case
when cat_id in (50013052,50008144) then "白酒"
when cat_id in (50013003,50008143,50013004,50512003) then "红酒"
when cat_id in (50008146,50013006) then "啤酒"
when cat_id = 50008147 then "黄酒"
when cat_id in (123256003,123502002,123214002,123224006) then "保健酒"
when cat_id = 50632001 then "预调酒"
when cat_id = 50008145 then "配制酒"
else "else" END,
user_id)t
group by wine_type

--4.2

select count(1) as num from
(select
user_id
from
t_base_ec_record_dev_new_simple
where
ds = 'true'
and
root_cat_id = 50008141
and
cat_id not in ("50008143","50013004","50013006","123224006","50013006","123214002","50013052","123502002","50008144","50008148","50008147")
group by
user_id
)t


--4.3

select count(1) as num from
(select
user_id
from
t_base_ec_record_dev_new_simple
where
ds = 'true'
and
root_cat_id = 50008141
group by
user_id
)t

-- 五粮液 = 4536492