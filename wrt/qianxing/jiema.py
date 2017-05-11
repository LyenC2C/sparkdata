#coding:utf-8

import csv
import sys

csv_file = sys.argv[1]
reader = csv.reader(file(csv_file, 'rb'))
n = 0
for line in reader:
    if n != 0:
        try:
            print "\001".join(ln.decode('gbk').encode('utf-8') for ln in line)
        except:
            print "haha "+n
    n += 1
