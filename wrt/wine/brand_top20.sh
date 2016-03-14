#!/usr/bin/env bash
hive<<EOF

use wlbase_dev;

create table t_wrt_brand_4536492_sold
{
    tloc STRING;
    sold STRING
};

insert overwrite table t_wrt_brand_4536492_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536492)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;

create table t_wrt_brand_4536492_sales
{
    tloc STRING;
    sales STRING
};

insert overwrite table t_wrt_brand_4536492_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536492)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;


create table t_wrt_brand_4101168_sold
{
    tloc STRING;
    sold STRING
};

insert overwrite table t_wrt_brand_4101168_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4101168)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;

create table t_wrt_brand_4101168_sales
{
    tloc STRING;
    sales STRING
};

insert overwrite table t_wrt_brand_4101168_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4101168)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;








create table t_wrt_brand_8224326_sold
{
    tloc STRING;
    sold STRING
};


insert overwrite table t_wrt_brand_8224326_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 8224326)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;

create table t_wrt_brand_8224326_sales
{
    tloc STRING;
    sales STRING
};

insert overwrite table t_wrt_brand_8224326_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 8224326)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;



create table t_wrt_brand_3670389_sold
{
    tloc STRING;
    sold STRING
};

insert overwrite table t_wrt_brand_3670389_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 3670389)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;

create table t_wrt_brand_3670389_sales
{
    tloc STRING;
    sales STRING
};

insert overwrite table t_wrt_brand_3670389_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 3670389)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;







create table t_wrt_brand_4536490_sold
{
    tloc STRING;
    sold STRING
};

insert overwrite table t_wrt_brand_4536490_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536490)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;

create table t_wrt_brand_4536490_sales
{
    tloc STRING;
    sales STRING
};

insert overwrite table t_wrt_brand_4536490_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536490)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;






create table t_wrt_brand_4537002_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_4537002_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4537002)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_4537002_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_4537002_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4537002)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;







create table t_wrt_brand_308222371_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_308222371_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 308222371)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_308222371_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_308222371_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 308222371)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;



create table t_wrt_brand_11972125_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_11972125_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 11972125)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_11972125_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_11972125_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 11972125)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;



create table t_wrt_brand_565376057_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_565376057_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 565376057)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_565376057_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_565376057_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 565376057)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_4537006_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_4537006_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4537006)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_4537006_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_4537006_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4537006)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;



create table t_wrt_brand_4536485_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_4536485_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536485)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_4536485_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_4536485_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536485)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_653690004_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_653690004_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 653690004)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_653690004_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_653690004_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 653690004)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_56742_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_56742_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 56742)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_56742_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_56742_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 56742)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_104692524_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_104692524_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 104692524)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_104692524_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_104692524_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 104692524)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_4536640_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_4536640_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536640)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_4536640_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_4536640_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536640)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_895926020_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_895926020_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 895926020)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_895926020_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_895926020_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 895926020)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_64088949_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_64088949_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 64088949)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_64088949_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_64088949_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 64088949)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_5413560_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_5413560_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 5413560)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_5413560_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_5413560_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 5413560)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_4536999_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_4536999_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536999)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_4536999_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_4536999_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 4536999)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

create table t_wrt_brand_106096950_sold
{
    tloc STRING;
    sold STRING
};
insert overwrite table t_wrt_brand_106096950_sold
select tloc,sold from
(select t2.tloc,count(1) as sold from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 106096950)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sold desc limit 20;




create table t_wrt_brand_106096950_sales
{
    tloc STRING;
    sales STRING
};
insert overwrite table t_wrt_brand_106096950_sales
select tloc,sales from
(select t2.tloc, sum(t1.price) as sales from
(select item_id,user_id,price from t_wrt_record_wine_12_02 where brand_id = 106096950)t1
join
(select tb_id,tloc from t_base_user_info_s)t2
on
t2.tb_id = t1.user_id
group by tloc
)t
order by sales desc limit 20;

EOF