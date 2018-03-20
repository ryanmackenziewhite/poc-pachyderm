#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Use Pandas dataframe to perform blocking
Extends the simple example to use 
data with multiple observations per datum

The following is already provided in the RecordLinkage
package.
"""
import argparse
import sys
from file_util import FileUtil
import pandas as pd
import numpy as np

def load_data(path,dsetname=''):
    util = FileUtil(path)
    util.walk()
    for key in util.datums:
        print(key, util.datums[key])
        if(len(dsetname)>0):
            fname = util.datums[key].split('/')[-1]
            print(fname)
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
                                            "street_number": object,
                                            "date_of_birth": object,
                                            "soc_sec_id": object,
                                            "postcode": object
                                            })
    return data

def select(df, pairs, level, drop=False):
    '''
    Using MultiIndex, select the matched records 
    and output new dataframe
    See recordlinkage/utils.py frame_indexing
    Select on label with 'loc'
    or select on position with 'iloc'
    Note that we drop duplicates, should be able to
    filter out directly. In record linkage, the two dataframes
    are created with all possible combinations (cross),
    in order to compute the features on two columns
    '''
    print(type(pairs.get_level_values(level)))
    data = df.loc[pairs.get_level_values(level)]
    data = data.drop_duplicates()
    return data

def write(df,name):
    df.to_csv(name)

def blocking_pd(df_a, df_b):
    '''
    Record Linkage makes use of the pandas MultiIndex
    Create a multiindex of original frames 
    after merging on column with same values
    see recordlinkage/index.py (Block)
    '''
    blocking_keys = 'given_name'
    data_left = pd.DataFrame(df_a[blocking_keys])
    data_left.columns = [blocking_keys]
    data_left['index_x'] = np.arange(len(df_a))
    data_left.dropna(axis=0, how='any', subset=[blocking_keys], inplace=True)
    
    data_right = pd.DataFrame(df_b[blocking_keys])
    data_right.columns = [blocking_keys]
    data_right['index_y'] = np.arange(len(df_b))
    data_right.dropna(axis=0, how='any', subset=[blocking_keys], inplace=True)

    pairs_df = data_left.merge(data_right, how='inner', on=[blocking_keys])

    return pd.MultiIndex(
            levels=[df_a.index.values, df_b.index.values],
            labels=[pairs_df['index_x'].values, pairs_df['index_y'].values],
            verify_integrity=False)

def link(pathA,pathB,dsetnameA='',dsetnameB=''):
    dfA = load_data(pathA,dsetnameA)
    dfB = load_data(pathB,dsetnameB)
    idx = blocking_pd(dfA, dfB)
    print(len(idx))
    df_blockA = select(dfA,idx,0,True)
    df_blockB = select(dfB,idx,1,True)
    print(len(dfA), len(df_blockA))
    print(len(dfB), len(df_blockB))

    write(df_blockA,"subsetA.csv")
    write(df_blockB,"subsetB.csv")


if __name__ == '__main__':
    print("Python Record Linkage Toolkit")
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', dest='inpathA', 
                        action='store', required=True, help='Input path')
    parser.add_argument('-b', dest='inpathB', 
                        action='store', required=True, help='Input path')
    parser.add_argument('-na', dest='dsetnameA',required=False, 
                        default='', help='Dataset')
    parser.add_argument('-nb', dest='dsetnameB',required=False, 
                        default='', help='Dataset')
    arguments = parser.parse_args(sys.argv[1:])    
    print(arguments.inpathA)
    link(arguments.inpathA, arguments.inpathB, 
            arguments.dsetnameA, arguments.dsetnameB)

