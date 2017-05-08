#coding:utf-8

import csv
import glob
import sys

csv_file = glob(sys.argv[1])
reader = csv.reader(file(csv_file, 'rb'))
for line in reader:
    print "\001".join(line)