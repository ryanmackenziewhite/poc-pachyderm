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
from file_util import FileUtil
import pandas as pd
import csv

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
        write_meta(list(data.columns.values),key)
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
    dfA = load_data(pathA, dsetnameA)
    print("Records: ", len(dfA))
    write(dfA, outpath + 'input_valid')   
    

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
