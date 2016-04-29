#coding:utf-8

'''
16145434155鹭尚无纺布袋子定做手提袋订做环保袋定制广告购物空白袋现货印字264538954412431466751物美价廉，赞一个2016.01.29 19:54:10true1454396880
'''
def f(x):
    ls = x.split("\001")
    ls[5] = ls[5].replace(".","-")[:10]
    if ls[6] == "true" or ls[6] == '1':
        ls[6] = '1'
    else:
        ls[6] = '0'
    return '\001'.join(ls)

s = '16145434155鹭尚无纺布袋子定做手提袋订做环保袋定制广告购物空白袋现货印字264538954412431466751物美价廉，赞一个2016.01.29 19:54:10true1454396880'
print f(s)

dic = {}
for line in open("XXX","r"):
	ls = line.strip().split(" ")
	for pair in ls:
		k,v = pair.split(":")
		if dic.has_key(k):
			dic[k].add(v)
		else:
			dic[k]=set([v])

for k in dic:
	if len(dic[k]) > 1:
		print k