create table wlservice.t_wrt_caiji_search_newitemid
select t1.* from
wlbase_dev.t_base_ec_search_item t1
left join
(select item_id from t_base_ec_item_dev_new where ds = 20161217) t2
on
t1.item_id = t2.item_id
where t2.item_id is null;


-- 20170104 库里采集到的一部分数据去重之后,与商品表join,商品表里没有的商品id给采集组采集
drop table wl_analysis.t_wrt_caiji_serach_newid;
create table wl_analysis.t_wrt_caiji_serach_newid as
select t1.nid,t1.ct from
(select nid,cast(comment_count as int) as ct from wl_base.t_base_item_search where ds = '20170124')t1
left join
(select item_id from wl_base.t_base_ec_item_dev_new where ds = 20170121)t2
on
t1.nid = t2.item_id
where
t2.item_id is null
order by t1.ct desc;





-- 20161227“关键词词搜索商品”而产生的“新商品”，新商品按照销量，BC去排列提供给辛成
-- def f(line):
--     ss = line.strip().split("\t",2)
--     if len(ss) != 3: return None
--     item_id = ss[1]
--     ob = json.loads(valid_jsontxt(ss[2]))
--     if type(ob) != type({}):return None
--     totalSoldQuantity = ob.get('apiStack',{}).get('itemInfoModel',{}).get('totalSoldQuantity','0')
--     # if valid_jsontxt(totalSoldQuantity) == '0'
--     trackParams = ob.get('trackParams',{})
--     BC_type = trackParams.get('BC_type','-')
--     if BC_type == '-': return None
--     result = []
--     result.append(valid_jsontxt(item_id))
--     result.append(valid_jsontxt(BC_type))
--     result.append(valid_jsontxt(totalSoldQuantity))
--     return '\001'.join(result)
--
-- rdd1 = sc.textFile('/commit/iteminfo/archive/20161224_arc/10.2.4.173_002590aded74.2016-12-2*')
-- rdd2 = sc.textFile('/commit/iteminfo/2016*/*')
-- rdd = rdd1.union(rdd2)
-- rdd.map(lambda x:f(x)).filter(lambda x:x!=None).saveAsTextFile("/user/wrt/temp/newiteminfo")
create table t_wrt_caiji_itemid_bc_sold_20161227 (itemid string,bc_type string,totalsold int);
load data inpath '/user/wrt/temp/newiteminfo' overwrite into table t_wrt_caiji_itemid_bc_sold_20161227;

--去重                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        65
drop table t_wrt_caiji_itemid_bc_sold_20161227_uniq;
create table t_wrt_caiji_itemid_bc_sold_20161227_uniq as
select itemid,max(bc_type) as bc_type,max(sold)  as sold from t_wrt_caiji_itemid_bc_sold_20161227 group by itemid;

--有销量且为淘宝
drop table wlservice.t_wrt_caiji_itemid_c_sold_20161227_issold;
create table t_wrt_caiji_itemid_c_sold_20161227_issold as
select * from
(select * from t_wrt_caiji_itemid_bc_sold_20161227_uniq where bc_type = 'C' and sold <> 0)t
order by sold desc;

--有销量且类型未知

drop table wlservice.t_wrt_caiji_itemid_no_sold_20161227_issold;
create table t_wrt_caiji_itemid_no_sold_20161227_issold as
select * from
(select * from t_wrt_caiji_itemid_bc_sold_20161227_uniq where bc_type = '-' and sold <> 0)t
order by sold desc;

--有销量且为天猫
drop table wlservice.t_wrt_caiji_itemid_b_sold_20161227_issold;
create table t_wrt_caiji_itemid_b_sold_20161227_issold as
select * from
(select * from t_wrt_caiji_itemid_bc_sold_20161227_uniq where bc_type = 'B' and sold <> 0)t
order by sold desc;






