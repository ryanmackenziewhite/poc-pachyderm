#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Script to merge data subsets into single file
for validation purposes
"""

import argparse
import sys
import os
from file_util import FileUtil
import pandas as pd
import csv
from shutil import copyfile
import glob

def write_meta(meta,name):
    metaname = '/pfs/out/'+name+'.meta.csv'

    with open(metaname, 'x') as f:
        writer = csv.writer(f)
        writer.writerow(meta)

def load_data(path, dsetname=''):
    util = FileUtil(path)
    util.walk()
    for key in util.datums:
        if(len(dsetname) > 0):
            fname = util.datums[key].split('/')[-1]
            if(dsetname in fname):
                print('Load ', key)
                #data = pd.read_csv(util.datums[key],
                #           header=None) 
                #write_meta(list(data.columns.values),key)
                
                copyfile(util.datums[key],)
            else:
                print(dsetname, key)
    return data

def write(df, name):
    dsetname = name + '.csv' 
    df.to_csv(dsetname, header = False)

def merge(pathA, dsetnameA='', outpath='./'): 
        
    '''
    Following method provides example 
    linking of two datasets
    '''

    # Load the datasets dataframes
    util = FileUtil(pathA)
    util.walk()
    for key in util.datums:
        if(len(dsetnameA) > 0):
            fname = util.datums[key].split('/')[-1]
            if(dsetnameA in fname):
                print('Copy ', fname)
                copyfile(util.datums[key], '/pfs/out/output_valid.csv')
            if('.features.csv' in fname):
                infile = util.datums[key]
                outfile = '/pfs/out/'+fname
                try:
                    os.symlink(infile,outfile)
                except:
                    print('Cannot create sim-link',
                            infile,
                            outfile)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', dest='inpathA', 
                        action='store', required=True, help='Input path')
    parser.add_argument('-na', dest='dsetnameA', required=False, 
                        default='', help='Dataset')
    parser.add_argument('-o', dest='output', default='./',required=False)
    arguments = parser.parse_args(sys.argv[1:])    
    print(arguments.inpathA)
    merge(arguments.inpathA, 
         arguments.dsetnameA, 
         arguments.output)

