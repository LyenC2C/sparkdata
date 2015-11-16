__author__ = 'wrt'
#coding:utf-8
def valid_jsontxt(content):
    if type(content) == type(u""):
        return content.encode("utf-8")
    else:
        return content

def pro_city(line):
    ln = valid_jsontxt(line).strip().replace("省","")
    province = ["北京","天津","上海","重庆","河北","河南","云南","辽宁","黑龙江","湖南","安徽","山东","新疆","江苏","浙江","江西","湖北","广西","甘肃","山西","内蒙古","陕西","吉林","福建","贵州","广东","青海","西藏","四川","宁夏","海南","台湾","香港","澳门"]
    for p in province:
        if (p in ln) and (p[0] == ln[0]):       #防止有些城市的名字和省的名字相同
            city = ln[len(p):]
            return (p,city)
    return (ln,"")
