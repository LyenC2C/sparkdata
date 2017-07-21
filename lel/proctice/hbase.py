import happybase as hb
import copy

#
conn = hb.Connection(host="master")
'''
pool = hb.ConnectionPool(size=3, host='master')
conn = pool.connection()
'''

# conn.create_table("happy",{"info":dict(max_versions=3,in_memory=True,block_cache_enabled=True),"detail":dict(max_versions=2)})

happy = conn.table("happy")
# happy.put("Lyen", {"info:name": "Lyen", "info:age": "22", "info:gender": "male", "detail:hobby": "basketball"})
# happy.put("Lyen", {"info:name": "Lyen", "info:age": "23", "info:gender": "male", "detail:hobby": "basketball"})
# happy.put("Good", {"info:name": "Good", "info:age": "25", "info:gender": "male", "detail:hobby": "pingpangball"})
'''
with happy.batch(batch_size=1000) as b:
    b.put('Jerry', {"info:age": "18", "info:gender": "male", "detail:hobby": "basketball"})
    b.put('Curry', {"info:age": "16", "info:gender": "female", "detail:hobby": "singing"})
    b.put('Mike', {"info:age": "18", "info:gender": "male", "detail:hobby": "basketball"})
    b.put('Jone', {"info:age": "30", "info:gender": "male", "detail:hobby": "computer games","detail:occupation":"gamer"})
    b.send()
'''

'''
print happy.row("Lyen",columns=["detail:hobby","info:age"])
for k,v in happy.rows(["Lyen","Curry"],columns=["detail:hobby","info:age"]): print k,v
for data,ts in happy.cells('Good', 'detail:hobby', versions=2,include_timestamp=True): print "Cell data at %s: %d" % (data,ts)
'''

for k, v in happy.scan(filter="SingleColumnValueFilter('info','age',>,'binary:20')"):
    print k,v
