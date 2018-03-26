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
from Levenshtein import distance 

def load_data(path, dsetname=''):
    util = FileUtil(path)
    util.walk()
    for key in util.datums:
        print(key, util.datums[key])
        if(len(dsetname) > 0):
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

def _compute(data1, data2):
    conc = pd.Series(list(zip(data1, data2)))

    def levensthein(x):
        try:
            return distance(x[0], x[1])
        except:
            return 999.
    
    return conc.apply(levensthein)

def compute(df1, df2):
    '''
    compute features on columns
    '''
    data1 = df1['given_name']
    data2 = df2['given_name']
    result = _compute(data1, data2)
    result.index = data1.index
    return result

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
    data = df.loc[pairs.get_level_values(level)]
    data.index = pairs
    return data


def write(df, name):
    df.to_csv(name, header = False)


def blocking_pd(df_a, df_b):
    '''
    Record Linkage makes use of the pandas MultiIndex
    Create a multiindex of original frames 
    after merging on column with same values
    see recordlinkage/index.py (Block)
    '''
    blocking_keys = 'soc_sec_id'
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


def link(pathA, pathB, dsetnameA='', dsetnameB='', outpath='./'):
    dfA = load_data(pathA, dsetnameA)
    dfB = load_data(pathB, dsetnameB)
    idx = blocking_pd(dfA, dfB)
    print(len(idx))
    df_blockA = select(dfA, idx, 0, True)
    df_blockB = select(dfB, idx, 1, True)
    print(len(dfA), len(df_blockA))
    print(len(dfB), len(df_blockB))
    
    dist = compute(df_blockA, df_blockB)
    print(len(dist))
    print(type(dist))
    #df_blockA['distance'] = dist
    #df_blockA = pd.concat([df_blockA, dist], axis=0)
    
    df_blockA = df_blockA.merge(dist.to_frame(), left_index=True, right_index=True)
    df_blockA.columns.values[-1] = 'distance'
    print(df_blockA)
    print(df_blockA.index.values)
    print(dist.index.values)
    

    write(df_blockA, outpath + 'subsetA.csv')
    write(df_blockB, outpath + 'subsetB.csv')


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
    parser.add_argument('-o', dest='output', default='./',required=False)
    arguments = parser.parse_args(sys.argv[1:])    
    print(arguments.inpathA)
    link(arguments.inpathA, arguments.inpathB, 
         arguments.dsetnameA, arguments.dsetnameB, arguments.output)

