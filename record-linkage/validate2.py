#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Validation script
"""
import argparse
import sys
import os
import csv
from file_util import FileUtil
import pandas as pd
from pandas.util.testing import assert_frame_equal

def get_index(arr):
    '''
    extract just the record id number from index
    '''
    idx=[]
    for val in arr:
        vals = val.split('-')
        if(len(vals) != 3):
            print('Error: ', vals)
        else:
            idx.append(vals[1])
    print('Total length of indices', len(idx))
    return list(map(int,idx))

def validate(dforg,dfvalid):
    equal = dforg.equals(dfvalid)
    print(equal)
    print(assert_frame_equal(dforg,dfvalid, check_dtype=False))
    return equal

def validate_meta(path):
    util = FileUtil(path)
    util.walk()
    meta=[]
    for key in util.datums:
        parts = key.split('.')
        for part in parts:
            if part != 'meta':
                continue
            with open(path + '/' + key, 'r') as f:
                reader = csv.reader(f)
                if(len(meta)==0): meta = next(reader)
                for row in reader:
                    if meta != row:
                        print('Meta data does not match', meta, row)
                        return []
    return meta




if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', dest='inpathA', 
                        action='store', required=True, help='Input path')
    parser.add_argument('-b', dest='inpathB', 
                        action='store', required=True, help='Input path')
    parser.add_argument('-na', dest='dsetnameA', required=False, 
                        default='', help='Dataset')
    parser.add_argument('-nb', dest='dsetnameB', required=False, 
                        default='', help='Dataset')
    arguments = parser.parse_args(sys.argv[1:])    

    dset_input = arguments.inpathA + '/input_valid.csv'
    dset_output = arguments.inpathB + '/output_valid.csv'
    df_in = pd.read_csv(dset_input,
                       header=None)
    df_out = pd.read_csv(dset_output,
                       header=None)
    df_out = df_out.drop(df_out.columns[1],axis=1)
    
    meta = validate_meta(arguments.inpathA)
    if(len(meta) != 0):
        meta.insert(0,'record_id')
    print(meta)
    print(df_in.head(10))
    print(df_out.head(10))
    df_in.columns = meta
    df_out.columns = meta
    df_in.set_index('record_id')
    df_out.set_index('record_id')
    print(len(df_in))
    print(len(df_out))
    if(len(df_in)==len(df_out)):
        is_valid = validate(df_in,df_out)
        if(is_valid):
            os.symlink(arguments.inpathB + '/features.csv', 
                       '/pfs/out/features.csv')



    
