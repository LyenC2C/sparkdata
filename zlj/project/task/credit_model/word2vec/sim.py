#coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from  gensim.models  import *
model=Word2Vec.load_word2vec_format('1130vec.bin',binary=True)
lines=[line for line in open('./fraud_corpus') ]

words=[word.decode('utf-u') for word in lines]
for word in words:
	try:
	    f.write(word.encode('utf-8')+'--------------------------\n')
	    rs=model.similar_by_word(word, 50)
	    f.write('\n'.join([k.encode('utf-8')+'   '+str(v) for k,v in rs]))
	    f.write('\n\n\n')

	except:pass