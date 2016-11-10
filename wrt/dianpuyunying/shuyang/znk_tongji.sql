--统计一个月前的老用户在近一个月中流失的数量（近一个月买过其他品牌纸尿裤，但没买过舒洋）
select count(1) from
(
  select new_other.user_id from
    (
    select aug_all.user_id as user_id from
      (
      select user_id from t_wrt_znk_record where ds = 20160829 and dsn > 20160800
      group by user_id
      )aug_all
    left join
      (
      select t2.user_id as user_id from
        (select item_id from t_wrt_znk_iteminfo_new where ds = 20160901 and brand_name = "舒洋")t1
      join
        (select user_id,item_id from t_wrt_znk_record where ds = 20160829 and dsn > 20160800)t2
      on
      t1.item_id = t2.item_id
      group by t2.user_id
      )aug_shuyang
    on
    aug_all.user_id = aug_shuyang.user_id
    where aug_shuyang.user_id is null
    )new_other
  join
    (
    select t2.user_id as user_id from
      (select item_id from t_wrt_znk_iteminfo_new where ds = 20160901 and brand_name = "舒洋")t1
    join
      (select user_id,item_id from t_wrt_znk_record where ds = 20160829 and dsn < 20160800)t2
    on
    t1.item_id = t2.item_id
    group by t2.user_id
    )old_shuyang
  on
  new_other.user_id = old_shuyang.user_id
)t

--老用户

select count(1) from
(
select user_id,buy_count from
(
select t2.user_id as user_id ,count(1) as buy_count  from
(select item_id from t_wrt_znk_iteminfo_new where ds = 20160901 and brand_name = "舒洋")t1
join
(select user_id,item_id from t_wrt_znk_record where ds = 20160829)t2
on
t1.item_id = t2.item_id
group by t2.user_id
)t
where buy_count > 1 --老用户为>1 新用户为=1
)tt