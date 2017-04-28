import os
str='''
ls /|awk '{print $1}';
ls /
'''
os.system(str)