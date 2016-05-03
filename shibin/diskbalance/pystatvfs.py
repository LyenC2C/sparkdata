#coding=utf-8
#author@shibin
#2015.11.19
from copy import deepcopy
from fs_util import dfpath
from fs_util import get_subdir_host, get_subdirs, get_blocks
from fs_util import safe_mv

block_size = 256 * 1024


#disk_meta
#devsd,allb,usedb,availb,precent,mnt,dir
def stat_dir(dirs):
    disk_infod = {}
    for dir in dirs:
        disk_meta = dfpath(dir)
        disk_meta.append(dir)
        devsd = disk_meta[0]
        disk_infod[devsd] = disk_meta

    disk_metas = disk_infod.values()
    disk_metas_sort = sorted(disk_metas, lambda a, b: cmp(a[3], b[3]))
    for disk_meta in disk_metas_sort:
        print disk_meta
    return disk_metas_sort


def calculate_disk(disk_metas):
    disk_count = len(disk_metas)
    disk_avails = [disk_meta[3] for disk_meta in disk_metas]

    disk_avail_avg = sum(disk_avails) / disk_count
    disk_avails_diff = [disk_avail - disk_avail_avg
                        for disk_avail in disk_avails]
    disk_block_diff = [disk_diff / block_size
                       for disk_diff in disk_avails_diff]

    print disk_block_diff
    return disk_block_diff


def balance_diff(disk_block_diff):
    mv_jobs = []
    start = 0
    end = len(disk_block_diff) - 1
    while start != end:
        s_block = disk_block_diff[start]
        e_block = disk_block_diff[end]
        mv_block = min(abs(s_block), abs(e_block))
        mv_jobs.append([mv_block, start, end])
        disk_block_diff[start] += mv_block
        disk_block_diff[end] -= mv_block
        if disk_block_diff[start] == 0:
            start += 1
        if disk_block_diff[end] == 0:
            end -= 1

    for job in mv_jobs:
        print job
    print disk_block_diff
    return mv_jobs


def explain_mv_jobs(disk_metas, mv_jobs):
    mv_details = []
    for job in mv_jobs:
        mv_block, mv_from, mv_to = job
        mv_detail = {}
        mv_detail['mv_from'] = disk_metas[mv_from][6]
        mv_detail['mv_to'] = disk_metas[mv_to][6]
        mv_detail['mv_block'] = mv_block
        mv_details.append(mv_detail)
        print mv_detail
    return mv_details


def do_mv_detail(mv_detail):
    mv_to = mv_detail['mv_to']
    mv_from = mv_detail['mv_from']
    mv_block = mv_detail['mv_block']

    to_host = get_subdir_host(mv_to)

    from_host = get_subdir_host(mv_from)
    from_subdirs = get_subdirs(from_host)

    for from_subdir in from_subdirs:
        if mv_block <= 0:
            break
        block_pairs = get_blocks(from_host, from_subdir)
        print mv_from, mv_block, from_subdir, len(block_pairs), mv_to
        for block_pair in block_pairs:
            try:
                istrue = safe_mv(from_host, to_host, from_subdir, block_pair)
                if istrue:
                    mv_block -= 1
            except Exception, e:
                print e
                return False
            if mv_block <= 0:
                break
    return True


if __name__ == '__main__':
    dirs = ['/mnt/hdfs/data%s/hadoop_data' % i for i in range(1, 8)]
    disk_metas = stat_dir(dirs)
    disk_block_diff = calculate_disk(disk_metas)
    mv_jobs = balance_diff(disk_block_diff)
    mv_details = explain_mv_jobs(disk_metas, mv_jobs)
    #mv_detail=mv_details[0]
    for mv_detail in mv_details:
        status = do_mv_detail(mv_detail)
        if status == False:
            break
