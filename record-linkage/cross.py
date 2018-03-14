#! /usr/bin/python3

"""
cross.py
Example of use of pachyderm to split dataset by line.
Take a cross product of two datasets.
Process each pair and apply simple blocking.
Intent is to show a simple example of record linkage,
relying on pachyderm to provide data and parallism.

Input data is cross of two datsets from pachyderm
If data is split per line, the input is expected to be
Two files w/ 1 row (record)
Each file comes from seperate repo

Blocking criteria is applied bby index of column
"""

import csv
import os
import sys
import argparse
import tempfile
from file_util import FileUtil


def blocking(data, idx):
    if(data[-1][idx] == data[-2][idx]):
        return True
    else:
        return False


def get_data(filepath, data):
    with open(filepath, 'r') as datum:
        reader = csv.reader(datum)
        for row in reader:
            data.append(row)
    return data            


def process(repoA, repoB):
    utilA = FileUtil(repoA)
    utilB = FileUtil(repoB)
    utilA.walk()
    utilB.walk()

    data = []
    for i, keyA in enumerate(utilA.datums.keys()):
        for j, keyB in enumerate(utilB.datums.keys()):
            fileA = utilA.datums[keyA]
            fileB = utilB.datums[keyB]
            get_data(fileA, data)
            get_data(fileB, data)
            if(blocking(data, 0) is True):
                nameA = 'recordA' + '_' + str(i)
                nameB = 'recordB' + '_' + str(j)
                tmpname = tempfile.mkdtemp(prefix='/pfs/out/')
                print('Files to link: ', fileA, os.path.join(tmpname, nameA))
                print('Files to link: ', fileB, os.path.join(tmpname, nameB))
                os.symlink(fileA, os.path.join(tmpname, nameA))
                os.symlink(fileB, os.path.join(tmpname, nameB))


if __name__ == '__main__':
    print("Executing blocking on cross")
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', dest='inpathA', 
                        action='store', required=True, help='Input path A')
    parser.add_argument('-b', dest='inpathB', 
                        action='store', required=True, help='Input path B')
    arguments = parser.parse_args(sys.argv[1:])
    process(arguments.inpathA, arguments.inpathB)
