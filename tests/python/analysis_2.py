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

CUR_FILE_PATH = os.path.dirname(__file__)
sys.path.append(CUR_FILE_PATH + '/../')
# from plt_mine import set_mpl

def set_mpl():
    mpl.rcParams.update(
        {
            'text.usetex': False,
            'font.sans-serif': 'Times New Roman',
            'mathtext.fontset': 'stix',
            'font.size': 20,
            'figure.figsize': (10.0, 10 * 0.618),
            'savefig.dpi': 10000,
            'axes.labelsize': 25,
            'axes.linewidth': 1.2,
            'xtick.labelsize': 20,
            'ytick.labelsize': 20,
            'legend.loc': 'upper right',
            'lines.linewidth': 2,
            'lines.markersize': 5

        }
    )
        
def read_log(file_name, data_len):
    f = open(file_name , 'r')
    line = f.readline()
    line = f.readline()
    datas = np.zeros((data_len, 20000000)) 
    
    count = 0
    while line:
        logs = line.split('|')
        for i in range(0, len(logs)):
            datas[i][count] = int(logs[i])
        count += 1
        line = f.readline()

    return datas, count

if __name__ == "__main__":
    # MMAP
    file_path_1 = '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-05-16-23:57:44/latency/0.log'
    # Buffer Pool overall
    file_path_2 = '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-05-17-09:14:46/latency/0.log'
    # Buffer Pool get and Decode
    file_path_3 = '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-05-17-09:20:49/latency/0.log'
    # Pread 
    file_path_4 = '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-05-16-22:50:37/latency/0.log'
    
    # datas_1, len_1 = read_log(file_path_1, 1)
    datas_2, len_2 = read_log(file_path_2, 5) # 735 ns
    datas_3, len_3 = read_log(file_path_3, 5)
    # datas_4, len_4 = read_log(file_path_4, 1)
    print(len_2)
    print(len_3)

    data_range = range(int(len_2/2), len_2)
    print("P99:", np.percentile(datas_2[0][data_range]/2.5, 50))
    print("P99:", np.percentile(np.array(datas_3[1][data_range])/2.5, 50))
    print("P99:", np.percentile(np.array(datas_3[3][data_range])/2.5, 50))


    
    print('work finished\n')


