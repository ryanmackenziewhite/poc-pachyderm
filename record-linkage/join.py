#! /usr/bin/python3

'''
Combine two datasets on key
Extract features
'''
import csv
import os
import sys
import argparse
from Levenshtein import distance 

def merge_data(data):
    '''
    Create single record
    '''
    result = data[0] + data[1][1:]
    dist = distance(data[0][0],data[1][0])
    print("Levenshtein distance: ", dist)
    result.append(dist)
    print(result)
  
def get_data(path):
    print("Collecting datums from ", path)
    data=[]
    for dirpath, dirs, files in os.walk(path):
        print(dirpath, dirs, files)
        for f in files:
            path_to_file = os.path.join(dirpath,f)
            print(path_to_file)
            with open(path_to_file,'r') as input:
                print('file open', path_to_file)
                reader = csv.reader(input)
                for row in reader:
                     data.append(row)
    if(len(data)>0):
       merge_data(data)
    else:
       print('No datums collected')

if __name__ == '__main__':
    print("Executing blocking on cross")
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', dest='inpath', 
                        action='store', required=True, help='Input path'  )
    arguments = parser.parse_args(sys.argv[1:])    
    print(arguments.inpath)
    get_data(arguments.inpath)
