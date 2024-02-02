#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 16:44:14 2023
@author: yz
"""

import os
import sys
import re
import numpy as np
from matplotlib import pyplot as plt
import matplotlib as mpl

import multiprocessing
import math

# CUR_FILE_PATH = os.path.dirname(__file__)
# sys.path.append(CUR_FILE_PATH + '/../')
# # from plt_mine import set_mpl

def decode_file(file_1):
    f1 = open(file_1, 'rb')
    line_start = 500
    line_num = 200
    line = f1.readline()
    
    throughput = 0
    while line:
        line_start -= 1
        if line_start <= 0:
            line_num -= 1
            logs = line.split()
            throughput += float(logs[0])
            if line_num == 0:
                break
        line = f1.readline()
    print(throughput/200)


if __name__ == "__main__":
    file_name_mmap_512_40 = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-01-15:53:53/log.log'
    file_name_mmap_512_50 = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-01-16:17:38/log.log'
    file_name_mmap_512_100 = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-01-16:33:11/log.log'
    
    file_name_mmap_1024_100 = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-01-16:52:15/log.log'
    file_name_mmap_1024_50 = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-01-17:20:28/log.log'
    file_name_mmap_1024_40 = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-01-17:45:27/log.log'
    file_name_mmap_1024_30 = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-01-18:08:01/log.log'
    tmp = '/home/spdk/p4510/zhengyang/data/graphscope_bufferpool/logs/2024-02-02-16:46:53/log.log'
    decode_file(tmp)
    # compare_file(file_name_1, file_name_2)
    # root_dir2 = '/data/zhengyang/data/server_side/experiment_space/LDBC_SNB/logs/2023-12-26-21:09:07/server/gs_log.log'
    # read_log(root_dir2)

    print('work finished\n')


