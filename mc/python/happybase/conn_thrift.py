#coding:utf8
import happybase
import rapidjson as json
conn = happybase.Connection(host='10.3.4.102',port=9090,timeout=2000)
print conn
table = conn.table('santai_order')
print(conn.tables())
row = table.row('18208106521_471161026')
for i in row:
    print i+"\t"+row.get(i)+"\n"