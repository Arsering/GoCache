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
        
def read_log(dir, data_len):
    datas = np.zeros((data_len, 20000000)) 
    count = 0

    for root, dirs, files in os.walk(dir):
        for file in files:
            # print(os.path.join(root, file))
            f = open(os.path.join(root, file) , 'r')
            line = f.readline()
            line = f.readline()

            while line:
                logs = line.split(' ')
                datas[0][count] = int(logs[1])
                count += 1
                line = f.readline()

    return datas, count

if __name__ == "__main__":
    # MMAP
    file_paths = ['/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-07-16-13:19:04/latency',
        '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-07-16-10:39:39/latency',
        '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-07-16-13:36:24/latency',
        '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-07-16-14:02:37/latency']
    data_id = 3
    data, count = read_log(file_paths[data_id], 1)
    
    start_time_point = [16298745230773750, 16272919270983525,
                        16301554155008488, 16305798992356052]
    print(count)
    print(start_time_point[data_id])
    
    print(f"P20:\t{np.percentile(np.array(data[0][0:count])-start_time_point[data_id], 20)/2.7/1000000}")
    print(f"P30:\t{np.percentile(np.array(data[0][0:count])-start_time_point[data_id], 30)/2.7/1000000}")
    print(f"P50:\t{np.percentile(np.array(data[0][0:count])-start_time_point[data_id], 50)/2.7/1000000}")
    print(f"P80:\t{np.percentile(np.array(data[0][0:count])-start_time_point[data_id], 80)/2.7/1000000}")
    print(f"P90:\t{np.percentile(np.array(data[0][0:count])-start_time_point[data_id], 90)/2.7/1000000}")
    print(f"P99.9:\t{np.percentile(np.array(data[0][0:count])-start_time_point[data_id], 99.9)/2.7/1000000}")

    print('work finished\n')


