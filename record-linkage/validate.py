#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import argparse
import sys
from file_util import FileUtil
import pandas as pd
import numpy as np
from Levenshtein import distance
import random
from pandas.util.testing import assert_frame_equal

def load_original(path, dsetname=''):
    util = FileUtil(path)
    util.walk()
    for key in util.datums:
        if(len(dsetname) > 0):
            fname = util.datums[key].split('/')[-1]
            if(fname != dsetname):
                continue
        print('Load ', key)
        data = pd.read_csv(util.datums[key],
                           index_col="rec_id",
                           sep=",",
                           engine='c',
                           skipinitialspace=True,
                           encoding='utf-8',
                           dtype={
                                  "given_name": str,
                                  "street_number": float,
                                  "date_of_birth": float,
                                  "soc_sec_id": int,
                                  "postcode": float
                                    })
    return data

def load_processed(path, dsetname, columns, dtypes):
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

def load_data(pathA, pathB, dsetnameA, dsetnameB):
    '''
    Load original data
    Set columns and dtypes for processed data frame
    '''
    dforg = load_original(pathA, dsetnameA)
    
    columns = list(dforg.columns.values)
    columns.insert(0,'rec_id')
    
    dtypes = dforg.dtypes.to_dict()
    
    idxorg = get_index(dforg.index.tolist())
    
    dforg['id'] = idxorg
    dforg = dforg.set_index('id')
    dforg = dforg.sort_index()
   
    dfvalid = load_processed(pathB,dsetnameB, columns, dtypes)
    idxvalid = get_index(dfvalid.index.tolist())
    dfvalid['id'] = idxvalid
    dfvalid = dfvalid.set_index('id')
    dfvalid = dfvalid.sort_index()

    return dforg, dfvalid

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
    org, valid = load_data(arguments.inpathA, arguments.inpathB, 
         arguments.dsetnameA, arguments.dsetnameB) 
    validate(org, valid)
         
