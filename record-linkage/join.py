#! /usr/bin/python3

'''
Combine two datasets on key
Extract features
'''
import csv
import sys
import argparse
from Levenshtein import distance 
from file_util import FileUtil

def write(name, data):
    print('Write merged data')
    print(data)
    name = '/pfs/out/' + name + '.csv'
    with open(name, 'x') as f:
        writer = csv.writer(f)
        writer.writerow(data)


def merge_data(data):
    '''
    Create single record
    '''
    result = data[0] + data[1][1:]
    dist = distance(data[0][0], data[1][0])
    print("Levenshtein distance: ", dist)
    result.append(dist)
    return result
  

def get_data(path):
    util = FileUtil(path)
    util.walk()
    data = []
    name = ''
    for key in util.datums:
        print(key, util.datums[key])
        name = name + key
        with open(util.datums[key], 'r') as datum:
            reader = csv.reader(datum)
            for row in reader:
                data.append(row)
    if(len(data) > 0):
        result = merge_data(data)
        write(name, result)
    else:
        print('No datums collected')


if __name__ == '__main__':
    print("Executing blocking on cross")
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', dest='inpath', 
                        action='store', required=True, help='Input path')
    arguments = parser.parse_args(sys.argv[1:])    
    print(arguments.inpath)
    get_data(arguments.inpath)
