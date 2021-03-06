#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Example record linkage using the Python Record Linkage Toolkit
"""
import argparse
import sys
import recordlinkage
from recordlinkage.datasets import load_febrl4
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

def link(pathA,pathB,dsetnameA='',dsetnameB=''):
    dfA = load_data(pathA,dsetnameA)
    dfB = load_data(pathB,dsetnameB)

    # Indexation step
    indexer = recordlinkage.BlockIndex(on='given_name')
    pairs = indexer.index(dfA, dfB)

    # Comparison step
    compare_cl = recordlinkage.Compare()

    compare_cl.exact('given_name', 'given_name', label='given_name')
    compare_cl.string('surname', 'surname', method='jarowinkler', threshold=0.85, label='surname')
    compare_cl.exact('date_of_birth', 'date_of_birth', label='date_of_birth')
    compare_cl.exact('suburb', 'suburb', label='suburb')
    compare_cl.exact('state', 'state', label='state')
    compare_cl.string('address_1', 'address_1', threshold=0.85, label='address_1')

    features = compare_cl.compute(pairs, dfA, dfB)

    # Classification step
    matches = features[features.sum(axis=1) > 3]
    print(len(matches))



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




