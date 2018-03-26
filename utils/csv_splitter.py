#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Standalone csv splitter, integrated with pypachy
Replacement for pachyderm put-file --split
One example found on codereview.stackexchange.com question 33193
"""
import argparse
from itertools import chain
import csv
import pypachy
import os
import sys
import io

def split_file(filename, pattern, size):
    """
    Split a file into multiple output files

    First line read from filename is header, 
    copied to each output file. Remaining blocks are split into 
    blocks of at least 'size' characters and written to output 
    files whose names are pattern.format(1), pattern.foramt(2), etc..
    The last file may be smaller
    """

    with open(filename, 'rb') as f:
        header = next(f)
        for index, line in enumerate(f, start=1):
            with open(pattern.format(index), 'wb') as out:
                out.write(header)
                n = 0
                for line in chain([line], f):
                    out.write(line)
                    n += len(f)
                    if n >= size:
                        break

class CSVSplitter(object):
    '''
    Read and generate file chunks
    Write chunks to new csv
    Consider use of multiprocessing to continue
    reading while writes occur 
    '''
    def __init__(self, filename, pattern, size, 
                 has_header = True, write_header = True, repo = '', branch = ''):
        self.filename = filename
        self.pattern = pattern 
        self.size = size
        self.has_header = has_header
        self.write_header = write_header
        self._header = None
        self._length = None
        self._client = pypachy.PfsClient()
        self._files = []
        self._repo = repo
        self._branch = branch
    
    def chunker(self, data):
        return iter(lambda: data.read(self.size),b'')
    
    def split_file(self):
        self._length = 0
        index = 0
        print('Write header ', self.write_header) 
        if(self._repo):    
            print('Commit to pfs ', self._repo)
            commit = self._client.start_commit(self._repo,self._branch)
        with open(self.filename, 'rb') as data: 
            if(self.has_header is True):
                self._header = next(data)
                print(self._header)
            for chunk in self.chunker(data):
                index +=1
                if(self.write_header is True):
                    chunk = self._header + chunk
                self._files.append(self.pattern.format(index))
                if(self._repo):
                    loc = '/'+self.pattern.format(index)
                    print('Commit file ', loc)
                    self._client.put_file_bytes(commit,loc,chunk)
                else:
                    print(self.pattern.format(index))
                    with open(self.pattern.format(index), 'wb') as out:
                        out.write(chunk)    
        if(self._repo):
            self._client.finish_commit(commit)


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', dest='input', action='store', 
                        required=True, help='Input file')
    parser.add_argument('-o', dest='output', action='store',
                        required=False, default='part', 
                        help='Optional output prefix')
    parser.add_argument('-c', dest='chunk', required=False, type = int, default=40960,
                        help='Optional split size')
    parser.add_argument('-r', dest='repo', default = '', help="Output repository")
    parser.add_argument('-b', dest='branch', default = '', help="Branch")
    parser.add_argument('-m', dest='hasheader', default = True, type = bool, help='Read header')
    parser.add_argument('-w', dest='writeheader', default = True, type = bool, help='Write header')
    
    arguments = parser.parse_args(sys.argv[1:])
    print(arguments)
    inputfile = arguments.input.split('.')
    print(inputfile)
    filepart = arguments.output + '_{0:0d}.csv'
    print(filepart)
    
    splitter = CSVSplitter(arguments.input,
                           filepart, 
                           arguments.chunk, 
                           arguments.hasheader,
                           arguments.writeheader,
                           arguments.repo,
                           arguments.branch)
    splitter.split_file()
    print(splitter._length)
    print(splitter._files)


