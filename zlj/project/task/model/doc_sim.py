# coding:utf-8
__author__ = 'zlj'
import sys

reload(sys)
sys.setdefaultencoding('utf8')

from gensim import corpora, models

dictionary = corpora.Dictionary.load('./mts.dic')

tfidf = models.TfidfModel.load('./mts.tfidf_model')

import math


def sim(doc1, doc2):
    vec_tfidf_doc1 = tfidf[dictionary.doc2bow(doc1.split())]
    vec_tfidf_doc2 = tfidf[dictionary.doc2bow(doc2.split())]
    s1 = sum([i[1] * i[1] for i in vec_tfidf_doc1])
    s2 = sum([i[1] * i[1] for i in vec_tfidf_doc1])
    d1 = {}
    d2 = {}
    for i in vec_tfidf_doc1:
        d1[i[0]] = i[1]
    for i in vec_tfidf_doc2:
        d2[i[0]] = i[1]
    value = 0
    for k, v in d1.iteritems():
        value = value + v * d2.get(k, 0)
    if s1 < 0.01 or s2 < 0.01: return 0
    return value / (math.sqrt(s1) * math.sqrt(s2))


def docsim(docs):
    dm = {}
    for i in xrange(len(docs)):
        for j in xrange(len(docs) - 1):
            dm[str(i) + '_' + str(j)] = sim(docs[i], docs[j + 1])
    s = set([i for i in xrange(len(docs))])
    ls = []
    while len(s) > 0:
        v = []
        i = s.pop()
        v.append(i)
        for j in s:
            if dm.get(str(i) + '_' + str(j), 0) > 0.9: v.append(j)
        for kv in v:
            if kv in s:
                s.remove(kv)
        ls.append(v)
    return ls
