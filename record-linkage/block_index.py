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
import csv
from file_util import FileUtil
import pandas as pd
import numpy as np
from Levenshtein import distance 

def load_data(path, dsetname=''):
    util = FileUtil(path)
    util.walk()
    for key in util.datums:
        if(len(dsetname) > 0):
            fname = util.datums[key].split('/')[-1]
            if(fname != dsetname):
                continue
        print('Load ', key)
        data = pd.read_csv(util.datums[key],
                           index_col="record_id",
                           sep=",",
                           engine='c',
                           skipinitialspace=True,
                           encoding='utf-8',
                           dtype={
                                  "StreetNumber": object,
                                  "DOB": object,
                                  "SIN": object,
                                  "PostalCode": object
                                    })
        break                   
    return data, key

def _compute(data1, data2):
    conc = pd.Series(list(zip(data1, data2)))

    def levensthein(x):
        try:
            return distance(x[0], x[1])
        except:
            return 999.
    
    return conc.apply(levensthein)

def compute(df1, df2, distance_key):
    '''
    compute features on columns
    '''
    print('Computing Levensthein distance on column ', distance_key)
    data1 = df1[distance_key]
    data2 = df2[distance_key]
    result = _compute(data1, data2)
    result.index = data1.index
    return result

def select(df, pairs, level):
    '''
    Using MultiIndex, select the matched records 
    and output new dataframe
    See recordlinkage/utils.py frame_indexing
    Select on label with 'loc'
    or select on position with 'iloc'
    '''
    data = df.loc[pairs.get_level_values(level)]
    data.index = pairs
    return data


def write(df, name):
    df.to_csv(name, header = False)

def write_meta(meta,name):

    with open(name, 'x') as f:
        writer = csv.writer(f)
        writer.writerow(meta)

def full_index(df_a, df_b):
    '''
    Creates a full cross product
    '''
    return pd.MultiIndex.from_product(
            [df_a.index.values, df_b.index.values])

def blocking_pd(df_a, df_b, blk_keys=[], validate = False):
    '''
    Record Linkage makes use of the pandas MultiIndex
    Create a multiindex of original frames 
    after merging on column with same values
    see recordlinkage/index.py (Block)
    '''
    if(blk_keys is None):
        print("warning, no blocking keys define")
        return None

    blocking_keys = blk_keys

    data_left = pd.DataFrame(df_a[blocking_keys])
    data_left.columns = blocking_keys
    data_left['index_x'] = np.arange(len(df_a))
    
    data_right = pd.DataFrame(df_b[blocking_keys])
    data_right.columns = blocking_keys
    data_right['index_y'] = np.arange(len(df_b))
    
    if(validate is True):    
        '''
        uses the record id index as a blocking key
        validate the distributed workload
        '''
        print('Validation: apply blocking on record id')
        data_left['id'] = data_left.index.values
        data_left.dropna(axis=0, how='any', 
                         subset=['id'], inplace=True)
        data_right['id'] = data_right.index.values 
        data_right.dropna(axis=0, how='any', 
                          subset=['id'], inplace=True)
        pairs_df = data_left.merge(data_right, how='inner', on=['id'])
    else:
        data_left.dropna(axis=0, how='any', 
                         subset=[blocking_keys], inplace=True)
        data_right.dropna(axis=0, how='any', 
                          subset=[blocking_keys], inplace=True)
        pairs_df = data_left.merge(data_right, 
                how='inner', on=[blocking_keys]) 

    return pd.MultiIndex(
            levels=[df_a.index.values, df_b.index.values],
            labels=[pairs_df['index_x'].values, pairs_df['index_y'].values],
            verify_integrity=False)


def link(pathA, pathB, 
         blk_keys=[], features=[], 
         dsetnameA='', dsetnameB='', 
         outpath='./', 
         validate=False, cross=False):
    '''
    Following method provides example 
    linking of two datasets
    '''

    # Load the datasets dataframes
    dfA, filekeyA = load_data(pathA, dsetnameA)
    dfB, filekeyB = load_data(pathB, dsetnameB)
    
    # Two options to produce a multi-index of two tables: 
    # 1) full cartesian cross product of pairs
    # 2) Inner join on a column

    if(cross is True):
        idx = full_index(dfA, dfB)
    else:
        idx = blocking_pd(dfA, dfB, blk_keys, validate)
    
    if idx is None:
        print('Warning, MultiIndex is None')

    # Apply the selection of records from each table
    df_blockA = select(dfA, idx, 0)
    df_blockB = select(dfB, idx, 1)
    
    write(df_blockA, outpath + 'output_valid.csv')
    # Compute Levenshtein distances for several columns
    distance_keys = features
    if(distance_keys is None):
        print('No distances to calcualte')
    else:
        print('Calculating Levenhstein distance on: ', features)
        for key in distance_keys:
            dist = compute(df_blockA, df_blockB, key)
            df_blockA = df_blockA.merge(dist.to_frame(), left_index=True, right_index=True)
            df_blockA.columns.values[-1] = key+'_dist'
    
    print('Original data set length ', len(dfA))
    print('Linking data set length ', len(dfB))
    print('Total linked pairs ', len(df_blockA))
    
    write(df_blockA, outpath + 'features.csv')
    write_meta(list(dfA.columns.values), 
               outpath + filekeyA + '.meta.csv')
    write_meta(list(df_blockA.columns.values), 
               outpath + filekeyA + '.features_meta.csv')


def validate(orig, new):
    '''
    Check input and output dataframes after join
    '''
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
    parser.add_argument('-o', dest='output', default='./',required=False)
    parser.add_argument('-v', dest='validate', default=False, 
                        type = bool, required=False)
    parser.add_argument('-c', dest='cross', default=False, 
                        type = bool, required=False, help='Full Index')
    parser.add_argument('-k', dest='keys', required=False, default=None,
                        action='store',nargs='+', help='Blocking keys')
    parser.add_argument('-f', dest='features', 
                        required=False,
                        action='store',
                        nargs='+', help='Columns for Features')
    # TODO
    # Add blocking keys and distance keys

    arguments = parser.parse_args(sys.argv[1:])    
    print(arguments.inpathA)
    print('Blocking keys: ', arguments.keys)
    print('Feature keys: ', arguments.features)
    link(arguments.inpathA, arguments.inpathB,
         arguments.keys, arguments.features,
         arguments.dsetnameA, arguments.dsetnameB, 
         arguments.output, arguments.validate, arguments.cross)

