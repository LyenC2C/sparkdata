#!/usr/bin/python
# encoding=utf-8

from ftplib import FTP
import sys, os.path


class MyFTP(FTP):
    '''  
    conncet to FTP Server  
    '''
    def __init__(self):
        pass
        # print 'make a object'
    def ConnectFTP(self, remoteip, remoteport, loginname, loginpassword):
        ftp = MyFTP()
        try:
            ftp.connect(remoteip, remoteport, 600)
            print 'success'
        except Exception, e:
            print >> sys.stderr, "conncet failed1 - %s" % e
            # return False
            return (0, 'conncet failed')
        else:
            try:
                ftp.login(loginname, loginpassword)
                print('login success')
            except Exception, e:
                print >> sys.stderr, 'login failed - %s' % e
                return (0, 'login failed')
                # return False
            else:
                print 'return 1'
                # return True
                return (1, ftp)

    def download(self, remoteHost, remotePort, loginname, loginpassword, remotePath, localPath):
        # connect to the FTP Server and check the return
        res = self.ConnectFTP(remoteHost, remotePort, loginname, loginpassword)
        if (res[0] != 1):
            print >> sys.stderr, res[1]
            return False

        # change the remote directory and get the remote file size
        ftp = res[1]
        ftp.set_pasv(0)
        dires = self.splitpath(remotePath)
        if dires[0]:
            ftp.cwd(dires[0])  # change remote work dir
        remotefile = dires[1]  # remote file name
        print dires[0] + ' ' + dires[1]
        fsize = ftp.size(remotefile)
        if fsize == 0:  # localfime's site is 0
            return False

        # check local file isn't exists and get the local file size
        lsize = 0L
        if os.path.exists(localPath):
            lsize = os.stat(localPath).st_size

        if lsize >= fsize:
            print 'local file is bigger or equal remote file'
            return False
        blocksize = 1024 * 1024
        cmpsize = lsize
        ftp.voidcmd('TYPE I')
        conn = ftp.transfercmd('RETR ' + remotefile, lsize)
        lwrite = open(localPath, 'ab')
        while True:
            data = conn.recv(blocksize)
            if not data:
                break
            lwrite.write(data)
            cmpsize += len(data)
            print 'download process:%.2f%%' % (float(cmpsize) / fsize * 100)
        lwrite.close()
        ftp.voidcmd('NOOP')
        ftp.voidresp()
        conn.close()
        ftp.quit()
        return True

    def upload(self, remotehost, remoteport, loginname, loginpassword, remotepath, localpath, callback=None):
        if not os.path.exists(localpath):
            print "Local file doesn't exists"
            return False
        self.set_debuglevel(2)
        res = self.ConnectFTP(remotehost, remoteport, loginname, loginpassword)
        if res[0] != 1:
            return False
        ftp = res[1]
        remote = self.splitpath(remotepath)
        ftp.cwd(remote[0])
        rsize = 0L
        try:
            rsize = ftp.size(remote[1])
        except:
            pass
        if (rsize == None):
            rsize = 0L
        lsize = os.stat(localpath).st_size
        print('rsize : %d, lsize : %d' % (rsize, lsize))
        if (rsize == lsize):
            print 'remote filesize is equal with local'
            return True
        if (rsize < lsize):
            localf = open(localpath, 'rb')
            localf.seek(rsize)
            ftp.voidcmd('TYPE I')
            datasock = ''
            esize = ''
            try:
                print(remote[1])
                datasock, esize = ftp.ntransfercmd("STOR " + remote[1], rsize)
            except Exception, e:
                print >> sys.stderr, '----------ftp.ntransfercmd-------- : %s' % e
                return False
            cmpsize = rsize
            while True:
                buf = localf.read(1024 * 1024)
                if not len(buf):
                    print '\rno data break'
                    break
                datasock.sendall(buf)
                if callback:
                    callback(buf)
                cmpsize += len(buf)
                print 'uploading %.2f%%' % (float(cmpsize) / lsize * 100)
                if cmpsize == lsize:
                    print '\rfile size equal break'
                    break
            datasock.close()
            # print 'close data handle'
            localf.close()
            # print 'close local file handle'
            ftp.voidcmd('NOOP')
            # print 'keep alive cmd success'
            ftp.voidresp()
            # print 'No loop cmd'
            ftp.quit()
            return True

    def splitpath(self, remotepath):
        position = remotepath.rfind('/')
        return (remotepath[:position + 1], remotepath[position + 1:])
UPLOAD = MyFTP()

# ftp.upload(host['host'],host['usr_name'],host['passwd'],sfpath,remote_path,host.get('port','21'))
def upload(server_host, usr_name, passwd, local_file_path, remote_file_path, port='21'):
    global UPLOAD
    try:
        return UPLOAD.upload(str(server_host), str(port), str(usr_name), str(passwd), str(remote_file_path),
                             str(local_file_path))
    except Exception, e:
        print str(e)
        return False

if __name__ == '__main__':
    # print os.path.join(path1,path2)

    print upload('ftp.blhdk.wolongdata.com', 'tes', 'passwd', 'localpath',
                 'uploadPath')