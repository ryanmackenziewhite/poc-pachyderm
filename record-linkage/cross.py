#! /usr/bin/python3

import csv
import os
import sys
import argparse
import tempfile

def blocking(data,idx):
    if(data[-1][idx]==data[-2][idx]):
        return True
    else:
        return False

def get_data(paths):
    filepaths=[]
    data=[]
    for p in paths:
        for dirpath, dirs, files in os.walk(p):
            for f in files:
                path_to_file=os.path.join(dirpath,f)
                filepaths.append(path_to_file)
                with open(path_to_file,'r') as input:
                    reader = csv.reader(input)
                    for row in reader:
                        data.append(row)
    return filepaths, data

def process(a, b):
    paths = [a,b]
    files, data = get_data(paths)
    print("Blocking on first field: ", files, data) 
    tmpname=tempfile.mkdtemp(prefix='/pfs/out/')
        
    if(blocking(data,0) is True):
       for i,f in enumerate(files):
           name = 'record' + '_' + str(i)
           print('Files to link: ', f, os.path.join(tmpname,name))
           os.symlink(f, os.path.join(tmpname,name))

if __name__ == '__main__':
    print("Executing blocking on cross")
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', dest='inpathA', 
                        action='store', required=True, help='Input path A'  )
    parser.add_argument('-b', dest='inpathB', 
                        action='store', required=True, help='Input path B'  )
    arguments = parser.parse_args(sys.argv[1:])
    process(arguments.inpathA, arguments.inpathB)
