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
        
def read_log(data_file):
    f = open(data_file+'/latency/0.log' , 'r')

    data = []
    count = 0
    line = f.readline()
    line = f.readline()

    while line:
        data.append(int(line))
        count += 1
        line = f.readline()
    print(sum(data[count-1001:count-1])/2.7/1000)


if __name__ == "__main__":
    # MMAP
    data_file = '/data/zhengyang/data/graphscope-flex/flex/graphscope_bufferpool/logs/2024-09-12-20:45:49'
    read_log(data_file)
    print('work finished\n')


