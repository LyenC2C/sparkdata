# coding: utf-8
from ftplib import FTP
timeout = 30
port = 21
ftp = FTP("ftp.blhdk.wolongdata.com","tes",'viN*Z5Hgc#2O8oBW')
#ftp.connect('ftp.blhdk.wolongdata.com',port,timeout) # 连接FTP服务器
#ftp.login('tes','viN*Z5Hgc#2O8oBW') # 登录
ftp.encoding='utf-8'
#下载
ftp.cwd('santai')    # 设置FTP路径
list = ftp.nlst()       # 获得目录列表
for name in list:
    print(name)             # 打印文件名字
    path = '/home/mc/project/santai/query/' + name    # 文件保存路径
    f = open(path,'wb')         # 打开要保存文件
    filename = 'RETR ' + name   # 保存FTP文件
    def write_ftp(str):
        f.write(str)
        #f.write(str.decode("gbk").encode("utf-8"))#从windows上传的文件乱码解决
#    ftp.retrbinary(filename,write_ftp,1024) # 保存FTP上的文件
#   ftp.delete(name)            # 删除FTP文件
#ftp.storbinary('STOR '+filename, open(path, 'rb')) # 上传FTP文件
#ftp.quit()                  # 退出FTP服务器