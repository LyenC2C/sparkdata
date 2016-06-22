--指定月份的各店铺商品前15名的销量与销售额

select shop_id,item_id,title,sold,sales,rn from
(
select shop_id,item_id,title,sold,sales,ROW_NUMBER() OVER (PARTITION BY shop_id ORDER BY sold desc) AS rn from
(
select item_id,shop_id,max(title) as title,cast(sum(sold) as int) as sold,sum(sales) as sales FROM
t_wrt_tmp_14shop_totalsold where ds > 20160500
group by item_id,shop_id)t
)tt
where rn <16 ;