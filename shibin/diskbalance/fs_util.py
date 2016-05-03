#coding=utf-8
#author@shibin
#2015.11.19

import commands
import sys
import re


def lspath(path):
    status, out = commands.getstatusoutput('ls ' + path)
    if status != 0:
        raise Exception("unable run  \" ls " + path +
                        " \" error ,please check it")
    return out


def get_subdir_host(dir):
    out = lspath(dir + '/current')
    for line in out.split('\n'):
        if 'BP' in line:
            host = dir + '/current/' + line + '/current/finalized'
            try:
                lspath(host)
            except:
                break
            print "find the dir: %s ,subdir host: %s" % (dir, host)
            return host
    raise Exception("there is'nt hadoop subdir host for the  dir:\" " + dir +
                    " \" ,please check it")


def get_subdirs(host):
    subdirs = []
    first_subdir = lspath(host).split('\n')
    for f_subdir in first_subdir:
        second_subdir = lspath(host + '/' + f_subdir).split('\n')
        for s_subdir in second_subdir:
            subdirs.append(f_subdir + '/' + s_subdir)
    return subdirs


def get_blocks(host, subdir):
    out = lspath(host + '/' + subdir)
    blockfiles = out.split('\n')
    block_pairs = []
    for blockfile in blockfiles:
        if 'meta' in blockfile:
            block_meta = blockfile
            block = block_meta[:block_meta.rfind('_')]
            if block in blockfiles:
                block_pairs.append([block, block_meta])

    return block_pairs


def copyfile(fromfile, tofile):
    status, out = commands.getstatusoutput("cp " + fromfile + "  " + tofile)
    if status != 0:
        print 'cp the file:failed:::' + fromfile + " " + tofile + " " + out
        return False, out
    return True, None


def rmfile(filepath):
    status, out = commands.getstatusoutput('rm ' + filepath)
    if status != 0:
        print 'rm the file:failed:::' + filepath + " " + out
        return False, out
    return True, None


def safe_mv(from_host, to_host, from_subdir, block_pair):

    block, block_meta = block_pair
    from_path = from_host + "/" + from_subdir
    to_path = to_host + "/" + from_subdir
    istrue_block = None
    istrue_block_meta = None

    needexit = None
    rmdown = None

    try:
        status, out = commands.getstatusoutput("mkdir -p " + to_path)
        print 'mv| try mv ' + from_path + "/" + block + ' to:' + to_path + "/" + block
        istrue_block, out = copyfile(from_path + "/" + block,
                                     to_path + "/" + block)
        print 'mv| try mv ' + from_path + "/" + block_meta + ' to:' + to_path + "/" + block_meta
        istrue_block_meta, out = copyfile(from_path + "/" + block_meta,
                                          to_path + "/" + block_meta)
        if istrue_block and istrue_block_meta:
            print 'mv| cp success:: and try rm the olds'
            rmfile(from_path + "/" + block)
            rmfile(from_path + "/" + block_meta)
            rmdown = True
            print 'mv| rm success:: this operate down'
            return True
    except KeyboardInterrupt, e:
        print ''
        print 'I know you want exit,please wait minutes', istrue_block, istrue_block_meta
        needexit = True
    #go on
    if istrue_block and istrue_block_meta:
        if not rmdown:
            print 'not down the job:rm olds '
            rmfile(from_path + "/" + block)
            rmfile(from_path + "/" + block_meta)
        print 'mv| this success rm olds '
    else:
        print 'this mv broke so find and try rm the new copy'
        #if istrue_block:
        #    print 'rm the  new block file:'+to_path+"/"+block
        #    rmfile(to_path+"/"+block)
        #if istrue_block_meta:
        #    print 'rm the  new block_meta file:'+to_path+"/"+block_meta
        #    rmfile(to_path+"/"+block_meta)
        rmfile(to_path + "/" + block)
        rmfile(to_path + "/" + block_meta)

    if not needexit:
        return False
    raise Exception("!!sucess exit")


def countblock():
    pass


def dfpath(path):
    status, out = commands.getstatusoutput('df ' + path)
    if status != 0:
        raise Exception("run \" df " + path + " \" error ,please check it")
    disk_metas = re.sub("[ ]+", "\t", out.split('\n')[1]).split('\t')
    #blocks
    #all---used---available
    disk_metas[1:4] = map(int, disk_metas[1:4])
    #precent
    disk_metas[4] = int(disk_metas[4].replace("%", ""))
    return disk_metas


if __name__ == '__main__':
    if sys.argv[1] == "df":
        path = sys.argv[2]
        print dfpath(path)
    if sys.argv[1] == "ls":
        path = sys.argv[2]
        print lspath(path)
    if sys.argv[1] == "host":
        dir = sys.argv[2]
        print get_subdir_host(dir)
    if sys.argv[1] == "subdir":
        host = sys.argv[2]
        subdirs = get_subdirs(host)
        for s in subdirs:
            print s
