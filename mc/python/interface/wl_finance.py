#coding:utf8
import json
import re
import numpy as np

file_record=open('/home/mc/oper_record/add.teltbqq.tb.record','r')#处理文件
file_root_cat=open('/home/mc/oper_record/root_category','r')#一级类目
file_high_brand=open('/home/mc/oper_record/high_brand.format','r')#高端品牌
file_cat_avg=open('/home/mc/oper_record/cat_avg_consume.format','r')#品类均价
file_cat_map=open('/home/mc/oper_record/tag_cat_map.format','r')#类别判断所属标签
dk_fruad_keyword=open('/home/mc/oper_record/dk_fruad_keyword','r')#风险关键词
file_root_cat_map=open("/home/mc/oper_record/root_cat_map.format",'r')#一级类目更换，第二列换为第三列
root_cat_list=[]#一级类目
for i in file_root_cat:
    root_cat_list.append(i.strip())
high_brand={}#高端品牌
for i in file_high_brand:
    high_brand[i.strip()]=None
cat_avg={}#品类均价
for i in file_cat_avg:
    ob=i.strip().split('\001')
    if ob[2].replace('.','').isdigit():
        cat_avg[ob[1]]=round(float(ob[2]),4)
    else:
        cat_avg[ob[1]]=0
cat_map={}#类别标签
for i in file_cat_map:
    ob=i.strip().split('\001')
    cat_map[ob[0]]=ob[1]

keyword_arr=[]#风险关键词
for i in dk_fruad_keyword:
    keyword_arr.append(i.strip())
fruad_key="|".join(keyword_arr)
root_cat_map={}#一级类目名称替换
for i in file_root_cat_map:
    ob=i.strip().split("\001")
    root_cat_map[ob[1]]=ob[2]

person_read={}
#读取记录文件
for i in file_record:
    #print i
    lis=i.strip().split('\t')
    if lis[8] not in root_cat_list:
        lis[8]='其他'
    try:
        price=float(lis[5])
    except Exception,e:
        print e,i
        continue
    phone=lis[0].strip()
    root_category=lis[8]
    if lis[4]=='-':
        brand='other/其他'
    else:
        brand=lis[4]
    title =lis[3]
    time=int(lis[2])
    uid=lis[1]
    location=lis[6]
    category =lis[7]
    online=lis[9]
    if person_read.has_key(phone):
        #个人总消费
        person_read[phone]["cost_sum"]+=price
        #个人总消费次数
        person_read[phone]["cost_num"]+=1.0
        #品牌消费金额及次数
        if person_read[phone]["brand_price"].has_key(brand):
            person_read[phone]["brand_price"][brand]+=price
            person_read[phone]["brand_num"][brand]+=1.0
        else:
            person_read[phone]["brand_price"][brand]=price
            person_read[phone]["brand_num"][brand]=1.0
        #一级品类消费金额及次数
        if person_read[phone]["rcate_price"].has_key(root_category):
            person_read[phone]["rcate_price"][root_category]+=price
            person_read[phone]["rcate_num"][root_category]+=1.0
        else:
            person_read[phone]["rcate_price"][root_category]=price
            person_read[phone]["rcate_num"][root_category]=1.0
        #子类消费金额及次数
        if person_read[phone]["cate_price"].has_key(category):
            person_read[phone]["cate_price"][category]+=price
            person_read[phone]["cate_num"][category]+=1.0
        else:
            person_read[phone]["cate_price"][category]=price
            person_read[phone]["cate_num"][category]=1.0
        #个人月消费金额及次数
        if person_read[phone]["time_price"].has_key(time):
            person_read[phone]["time_price"][time]+=price
            person_read[phone]["time_num"][time]+=1.0
        else:
            person_read[phone]["time_price"][time]=price
            person_read[phone]["time_num"][time]=1.0
        #线上购物金额及次数
        if online=='B':
            person_read[phone]['online']['cost']+=price
            person_read[phone]['online']['cost_num']+=1.0
        #50元以下购物金额及次数
        if price<50:
            person_read[phone]['less50']['cost']+=price
            person_read[phone]['less50']['cost_num']+=1.0
        #贷款异常类购物记录次数
        if(len(re.findall(fruad_key,title))>0):
            person_read[phone]['fraud']+=1
    else:
        person_read[phone]={}
        person_read[phone]["cost_sum"]=price
        person_read[phone]["cost_num"]=1.0
        person_read[phone]["brand_price"]={}
        person_read[phone]["brand_price"][brand]=price
        person_read[phone]["brand_num"]={}
        person_read[phone]["brand_num"][brand]=1.0
        person_read[phone]["rcate_price"]={}
        person_read[phone]["rcate_price"][root_category]=price
        person_read[phone]["rcate_num"]={}
        person_read[phone]["rcate_num"][root_category]=1.0
        person_read[phone]["cate_price"]={}
        person_read[phone]["cate_price"][category]=price
        person_read[phone]["cate_num"]={}
        person_read[phone]["cate_num"][category]=1.0
        person_read[phone]["time_price"]={}
        person_read[phone]["time_price"][time]=price
        person_read[phone]["time_num"]={}
        person_read[phone]["time_num"][time]=1.0
        person_read[phone]['online']={}
        if online=='B':
            person_read[phone]['online']['cost']=price
            person_read[phone]['online']['cost_num']=1.0
        else:
            person_read[phone]['online']['cost']=0.0
            person_read[phone]['online']['cost_num']=0.0
        person_read[phone]['less50']={}
        if price<50:
            person_read[phone]['less50']['cost']=price
            person_read[phone]['less50']['cost_num']=1.0
        else:
            person_read[phone]['less50']['cost']=0.0
            person_read[phone]['less50']['cost_num']=0.0
        person_read[phone]['fraud']=0
        if(len(re.findall(fruad_key,title))>0):
            person_read[phone]['fraud']+=1

#result={}
file_out=open("/home/mc/oper_record/record20170329",'w')
for i in person_read:
    #品类偏好TOP5及其占比
    #result[i]={}
    result=[]
    result.append(str(i))
    r1=sorted(person_read[i]["rcate_price"].iteritems(),key = lambda d:d[1] ,reverse =True)[:5]
    rcate={}#品类偏好
    price_comp={}#价格分析
    for j in r1:
        rname=j[0]
        if root_cat_map.has_key(j[0]):
            rname=root_cat_map.get(j[0])
        rcate_avg=j[1]/person_read[i]["rcate_num"].get(j[0])
        price_comp[rname]=str(round((rcate_avg-cat_avg.get(j[0]))/cat_avg.get(j[0]),4)*100.0)+"%"
        rcate[rname]=str(round(j[1]/person_read[i]["cost_sum"],4)*100.0)+"%"
    #result[i]["rcate"]=rcate
    #result[i]["price_comp"]=price_comp
    result.append(json.dumps(rcate,ensure_ascii=False))
    #品牌偏好top5及。。
    r2=sorted(person_read[i]["brand_price"].iteritems(),key = lambda d:d[1] ,reverse =True)[:5]
    brand_dic={}
    for j in r2:
        brand_dic[j[0]]=str(round(j[1]/person_read[i]["cost_sum"],4)*100.0)+"%"
    #result[i]["brand"]=brand_dic
    result.append(json.dumps(brand_dic,ensure_ascii=False))
    #高端品牌百分比
    high_brand_num=0
    for j in person_read[i]["brand_price"]:
        if high_brand.has_key(j):
            high_brand_num+=person_read[i]["brand_price"].get(j)
    #result[i]["hbrand_ratio"]=str(round(high_brand_num/person_read[i]["cost_sum"],4)*100.0)+"%"
    result.append(str(round(high_brand_num/person_read[i]["cost_sum"],4)*100.0)+"%")
    result.append(json.dumps(price_comp,ensure_ascii=False))
    #各一级类目累计购物金额占比
    #各一级类目累计购物次数占比
    rcate_ratio_arr=[]
    rcate_ratio_num_arr=[]
    for j in range(len(root_cat_list)):
        rcate=root_cat_list[j]
        if person_read[i]["rcate_price"].has_key(rcate):
            rcate_ratio_arr.append(str(round(person_read[i]["rcate_price"].get(rcate)/person_read[i]["cost_sum"],4)*100.0)+"%")
            rcate_ratio_num_arr.append(str(round(person_read[i]["rcate_num"].get(rcate)/person_read[i]["cost_num"],4)*100.0)+"%")
        else:
            rcate_ratio_arr.append("0.0%")
            rcate_ratio_num_arr.append("0.0%")
    #result[i]["rcate_ratio"]=rcate_ratio_arr
    result.append(json.dumps(rcate_ratio_arr,ensure_ascii=False))
    #result[i]["rcate_num_atio"]=rcate_ratio_num_arr
    result.append(json.dumps(rcate_ratio_num_arr,ensure_ascii=False))
    #购物次数波动 标准差np.std()
    o_li=sorted(person_read[i]["time_num"].iteritems(),key = lambda d:d[0] ,reverse =False)
    month_gou_arr=[]
    for n in o_li:
        month_gou_arr.append(n[1])
    #result[i]['m_std_month_cnt']=round(np.std(month_gou_arr),2)
    result.append(str(round(np.std(month_gou_arr),2)))
    #线上商城类购物金额占比
    #result[i]["online_ratio"]=str(round(person_read[i]["online"]["cost"]/person_read[i]['cost_sum'],4))+"%"
    result.append(str(round(person_read[i]["online"]["cost"]/person_read[i]['cost_sum'],4)*100)+"%")
    #线上商城类购物次数占比
    #result[i]["online_num_ratio"]=str(round(person_read[i]["online"]["cost_num"]/person_read[i]['cost_num'],4))+"%"
    result.append(str(round(person_read[i]["online"]["cost_num"]/person_read[i]['cost_num'],4)*100)+"%")
    #有品牌购物金额占比
    #有品牌购次数占比
    have_brand_cost=0.0
    have_brand_num=0
    for m in person_read[i]["brand_price"]:
        if ("其他" in m or "其它" in m or "other" in m) == False:
            have_brand_cost+=person_read[i]["brand_price"].get(m)
            have_brand_num+=person_read[i]["brand_num"].get(m)
    #result[i]['have_brand_price_ratio']=str(round(have_brand_cost/person_read[i]["cost_sum"],4)*100.0)+"%"
    result.append(str(round(have_brand_cost/person_read[i]["cost_sum"],4)*100.0)+"%")
    #result[i]['have_brand_num_ratio']=str(round(have_brand_num/person_read[i]["cost_num"],4)*100.0)+"%"
    result.append(str(round(have_brand_num/person_read[i]["cost_num"],4)*100.0)+"%")
    #50元以下商品消费金额占比
    #result[i]['less50_price_ratio']=str(round(person_read[i]["less50"]["cost"]/person_read[i]['cost_sum'],4))+"%"
    result.append(str(round(person_read[i]["less50"]["cost"]/person_read[i]['cost_sum'],4)*100)+"%")
    #result[i]['less50_num_ratio']=str(round(person_read[i]["less50"]["cost_num"]/person_read[i]["cost_num"],4)*100.0)+"%"
    result.append(str(round(person_read[i]["less50"]["cost_num"]/person_read[i]["cost_num"],4)*100.0)+"%")
    #购物金额波动
    o_li2=sorted(person_read[i]["time_price"].iteritems(),key = lambda d:d[0] ,reverse =False)
    month_gou_arr2=[]
    for n in o_li2:
        month_gou_arr2.append(n[1])
    #result[i]['m_std_month_price']=round(np.std(month_gou_arr2),4)
    result.append(str(round(np.std(month_gou_arr2),2)))
    #各一级类目单笔消费金额与全网平均比值综合值
    cost_gen=0
    cost_gen_num=0
    for m in person_read[i]["rcate_price"]:
        cost_avg=round(person_read[i]["rcate_price"][m]/person_read[i]["rcate_num"][m],4)
        avg=cat_avg.get(m)
        if i=='18729601862':
            print m+'\t'+str(cost_avg)+'\t'+str(avg)
        if cost_avg==None or avg==None or float(cost_avg)==0.0:
            add_num=0
        else:add_num=(cost_avg*1.0)/avg
        cost_gen+=add_num
        cost_gen_num+=1
    #result[i]['m_general_ratio']=round(cost_gen/cost_gen_num,4)
    result.append(str(round(cost_gen/cost_gen_num,2)))
    #消费偏好
    xiaofei={}
    for j in person_read[i]['cate_price']:
        if cat_map.has_key(j):
            xiaofei[cat_map[j]]=1
    #result[i]['cost_map']=xiaofei
    result.append(json.dumps(xiaofei,ensure_ascii=False))
    #累计贷款类异常购买记录次数
    #result[i]["fraud"]=person_read[i]['fraud']
    result.append(str(person_read[i]['fraud']))
    file_out.write('\t'.join(result)+"\n")