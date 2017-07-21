import happybase as hb
import random

conn = hb.Connection(host="master")
'''
conn.create_table("pv",{"data":dict(max_versions=3,in_memory=True,block_cache_enabled=True),"ip":dict(max_versions=2)})
'''
pv = conn.table("pv")
ts = [1499066595, 1498980195, 1498893795]
net_type = ["http", "htts"]
ip = ["10.1.1.1", "10.1.1.2", "10.1.1.3", "10.1.1.224", "10.2.2.224","20.1.1.224","30.1.1.224"]
'''
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[0]]])), {"data:net_type": net_type[0], "ip:ip": ip[0]})
pv.put(str("-".join([str(i) for i in  [random.randint(0, 5), ts[1], ip[0]]])), {"data:net_type": net_type[0], "ip:ip": ip[0]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[0]]])), {"data:net_type": net_type[0], "ip:ip": ip[0]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[1]]])), {"data:net_type": net_type[0], "ip:ip": ip[1]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[1]]])), {"data:net_type": net_type[0], "ip:ip": ip[1]})
pv.put(str("-".join([str(i) for i in[random.randint(0, 5), ts[0], ip[2]]])), {"data:net_type": net_type[0], "ip:ip": ip[2]})
pv.put(str("-".join([str(i) for i in[random.randint(0, 5), ts[1], ip[2]]])), {"data:net_type": net_type[0], "ip:ip": ip[2]})
pv.put(str("-".join([str(i) for i in[random.randint(0, 5), ts[2], ip[2]]])), {"data:net_type": net_type[0], "ip:ip": ip[2]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[3]]])), {"data:net_type": net_type[0], "ip:ip": ip[3]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[3]]])), {"data:net_type": net_type[0], "ip:ip": ip[3]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[3]]])), {"data:net_type": net_type[0], "ip:ip": ip[3]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[2]]])), {"data:net_type": net_type[0], "ip:ip": ip[2]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[2]]])), {"data:net_type": net_type[0], "ip:ip": ip[2]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[2]]])), {"data:net_type": net_type[0], "ip:ip": ip[2]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[3]]])), {"data:net_type": net_type[0], "ip:ip": ip[3]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[3]]])), {"data:net_type": net_type[0], "ip:ip": ip[3]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[3]]])), {"data:net_type": net_type[0], "ip:ip": ip[3]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[4]]])), {"data:net_type": net_type[0], "ip:ip": ip[4]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[4]]])), {"data:net_type": net_type[0], "ip:ip": ip[4]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[4]]])), {"data:net_type": net_type[0], "ip:ip": ip[4]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[1]]])), {"data:net_type": net_type[0], "ip:ip": ip[1]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[1]]])), {"data:net_type": net_type[0], "ip:ip": ip[1]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[1]]])), {"data:net_type": net_type[0], "ip:ip": ip[1]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[5]]])), {"data:net_type": net_type[0], "ip:ip": ip[5]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[5]]])), {"data:net_type": net_type[0], "ip:ip": ip[5]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[5]]])), {"data:net_type": net_type[0], "ip:ip": ip[5]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[0], ip[6]]])), {"data:net_type": net_type[0], "ip:ip": ip[6]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[1], ip[6]]])), {"data:net_type": net_type[0], "ip:ip": ip[6]})
pv.put(str("-".join([str(i) for i in [random.randint(0, 5), ts[2], ip[6]]])), {"data:net_type": net_type[0], "ip:ip": ip[6]})

'''

'''
#BinaryComparator        ->binary
#BinaryPrefixComparator  ->binaryprefix
#RegexStringComparator   ->regexstring
#SubStringComparator     ->substring
'''

# for k, v in pv.scan(filter="RowFilter(=,'regexstring:.*-1498893795-.*')"): print k, v

for k, v in pv.scan(filter="RowFilter(<,'binary:1499066595')"): print k, v
