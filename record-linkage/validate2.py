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

def validate_df(dforg,dfvalid):
    equal = dforg.equals(dfvalid)
    print(equal)
    print(assert_frame_equal(dforg,dfvalid, check_dtype=False))
    print(dforg['record_id'].equals(dfvalid['record_id']))
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

def link_features(path):
    # Load the datasets dataframes
    util = FileUtil(path)
    util.walk()
    for key in util.datums:
        fname = util.datums[key].split('/')[-1]
        if('.features.csv' in fname):
            infile = util.datums[key]
            outfile = '/pfs/out/'+fname
            try:
                os.symlink(infile,outfile)
            except:
                print('Cannot create sim-link',
                        infile,
                        outfile)


def validation(df_in, df_out):
    is_valid = False
    if(len(df_in)==len(df_out)):
        is_valid = validate_df(df_in,df_out)
        if(is_valid):
            print("DF equal")
        else:
            print('Cannot validate tables')
            print("------- Input dataset -------- ")
            print(df_in.dtypes)
            print(df_in.head(10))
            print("------- Validation dataset -------- ")
            print(df_out.dtypes)
            print(df_out.head(10))
    else:
        print("Table sizes not equal")
        print("Input dataset length ", len(df_in))
        print("Output validation dataset length ", len(df_out))
    
    return is_valid    

def load_data(dset_input, dset_output):
    df_in = pd.read_csv(dset_input,
                       header=None,
                       dtype={0:str,
                           1:str,
                           2:str})
    df_out = pd.read_csv(dset_output,
                       header=None,
                       dtype={0:str,
                           1:str,
                           2:str})
    df_out = df_out.drop(df_out.columns[1],axis=1)

    return df_in, df_out

def prepare_tables(df_in, df_out, meta):
    df_in.columns = meta
    df_out.columns = meta
    
    idx_in = get_index(df_in['record_id'].tolist())
    idx_out = get_index(df_out['record_id'].tolist())
    
    df_in['idx'] = idx_in
    df_out['idx'] = idx_out
    df_in = df_in.set_index('idx')
    df_out = df_out.set_index('idx')
    
    df_in.sort_index(inplace=True)
    df_out.sort_index(inplace=True)

    return df_in, df_out

def analysis(repo_in, repo_out):
    dset_input = repo_in + '/input_valid.csv'
    dset_output = repo_out + '/output_valid.csv'
    print('Validation ', dset_input, dset_output) 
    meta = validate_meta(arguments.inpathA)
    if(len(meta) != 0):
        meta.insert(0,'record_id')
    print("----- Meta data validation -----")
    print(meta)
    
    df_in, df_out = load_data(dset_input, dset_output)

    df_in, df_out = prepare_tables(df_in, df_out, meta)
    
    is_valid = validation(df_in, df_out)
    return is_valid

if __name__ == '__main__':
    print("Validation of identical dataset record linkage")
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

    is_valid = analysis(arguments.inpathA, 
                        arguments.inpathB)

    print("Dataset validation ", is_valid)
    if is_valid:
        link_features(arguments.inpathB)
                             




    
