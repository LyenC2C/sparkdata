__author__ = 'zlj'

# f=open('/home/zlj/workspace/data/hmm/part-00000')
# fw=open('/home/zlj/workspace/data/hmm/crf/example/ner/test','w')
#
# for i,line in enumerate(f):
#     if i>100: break
#     try:
#         data=line.split('\t')
#         qq=data[0]
#
#         for words in data:
#             if ':' in words:
#                 name,key=words.split(':')
#                 # print (name)
#                 for s in name.decode('utf-8'):
#                 # for s in name:
#                 #     print s
#                     fw.write(s.encode('utf-8')+"\n")
#                 fw.write(''+"\n")
#
#     except:
#         pass
# fw.close()

# f1=open('/home/zlj/workspace/data/hmm/train_zi')
# fw=open('/home/zlj/workspace/data/hmm/crf/example/ner/train_corpus','w')
#
# for line in f1:
#     # print line.strip()
#     data=line.strip().split('\t')
#     for  index,word in enumerate(data):
#         if index==0:
#             fw.write(word+'\t'+'B\n')
#         else :fw.write(word+'\t'+'I\n')
#
#     fw.write(''+'\t'+'O\n')
# fw.close()



f1=open('/home/zlj/workspace/data/hmm/1')
import jieba.posseg as pseg
for line_i,line in enumerate(f1):
    if line_i>100: break
    # try:
    data=line.split('\t')
    qq=data[0]
    a={}
    for words in data:
        if ':' in words:
            name,key=words.split(':')
            words = pseg.cut(name)
            for word, flag in words:
                if(flag=='nr'):
                    if word in a:   a[word]=a[word]+1
                    else :  a[word]=1
                # print('%s %s' % (word, flag))
    dict= sorted(a.iteritems(), key=lambda d:d[1], reverse = True)
    if len(dict)>0:
        name,num=dict[0]
        print len(a),name ,num
    else: continue
    if(num>1):
        for words in data:
            if ':' in words:
                value,key=words.split(':')
                index=value.find(name.encode('utf-8'))

                if index>-1:
                    end =index+len(name)
                    for i,v in enumerate(value):
                        if i>index and i<end:
                            print v+" "+"B"
                        else: print v+" "+"O"
    # except: print 'er'

