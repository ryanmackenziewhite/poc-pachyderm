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
import csv
from file_util import FileUtil
import pandas as pd
from pandas.util.testing import assert_frame_equal

def load_processed(path, dsetname, meta):
    util = FileUtil(path)
    util.walk()
    for key in util.datums:
        if(len(dsetname) > 0):
            fname = util.datums[key].split('/')[-1]
            if(fname != dsetname):
                continue
        print('Load ', key)
        data = pd.read_csv(util.datums[key],
                           header=None)
        data = data.drop(data.columns[1],axis=1)
        data.columns = columns
        for key in dtypes:
            data[key] = data[key].astype(dtypes[key])

        data = data.set_index('rec_id')
                           
    return data

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
    pass

if __name__ == '__main__':
    print("Python Record Linkage Toolkit")
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
    print(arguments.inpathA)
    utilA = FileUtil(arguments.inpathA)
    utilA.walk()
    for key in utilA.datums:
        print(key)
    utilB = FileUtil(arguments.inpathB)
    utilB.walk()
    for key in utilB.datums:
        print(key)

    dset_input = arguments.inpathA + '/input_valid.csv'
    dset_output = arguments.inpathB + '/output_valid.csv'
    df_in = pd.read_csv(dset_input,
                       header=None)
    df_out = pd.read_csv(dset_output,
                       header=None)
    df_out = df_out.drop(df_out.columns[1],axis=1)
    
    meta = ['rec_id','given_name','surname','street_number',
            'address_1','address_2','suburb',
            'postcode','state','data_of_birth',
            'age','phone_number','soc_sec_id','blocking_number']
    print(df_in.head(10))
    print(df_out.head(10))
    df_in.columns = meta
    df_out.columns = meta
    df_in.set_index('rec_id')
    df_out.set_index('rec_id')
    print(len(df_in))
    print(len(df_out))
    validate(df_in,df_out)
    
