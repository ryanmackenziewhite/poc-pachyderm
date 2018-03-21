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
                    n += len(file)
                    if n >= size:
                        break

class CSVSplitter(object):
    '''
    Read and generate file chunks
    Write chunks to new csv
    Consider use of multiprocessing to continue
    reading while writes occur 
    '''
    def __init__(self, filename, pattern, size, has_header = True):
        self.filename = filename
        self.pattern = pattern 
        self.size = size
        self.has_header = has_header
        self._header = None
        self._length = None
        self._client = pypachy.PfsClient()
        self._files = []

    def chunker(self,reader):
        i = 0
        chunk = []
        for i, line in enumerate(reader):
            if(i % self.size == 0 and i >0):
                yield chunk
                del chunk[:]
            chunk.append(line)
        yield chunk   
    
    def get_schema(self,header):
        ncols = len(header)
        return ncols
    
    def split_file(self):
        self._length = 0
        index = 0
        use_buffer = True
        with open(self.filename, 'r') as data:
            if(self.has_header is True):
                reader = csv.reader(data)
                self._header = next(reader)
                for chunk in self.chunker(reader):
                    index +=1
                    self._files.append(self.pattern.format(index))
                    if(use_buffer):
                        output = io.StringIO()
                        writer = csv.writer(output)
                        writer.writerow(self._header)
                        writer.writerows(chunk)
                        with self._client.commit('test','master') as c:
                            loc = '/'+self.pattern.format(index)
                            #print(output.getvalue())
                            print(loc)
                            self._client.put_file_bytes(c,loc,output)
                    else:
                        with open(self.pattern.format(index), 'w') as out:
                            writer = csv.writer(out)
                            writer.writerow(self._header)
                            writer.writerows(chunk)

        print(self._client.get_files('test/master','/'))


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', dest='input', action='store', 
                        required=True, help='Input file')
    parser.add_argument('-o', dest='output', action='store',
                        required=False, default='part', 
                        help='Optional output prefix')
    parser.add_argument('-c', dest='chunk', required=False, default=1000,
                        help='Optional split size')
    parser.add_argument('-r', dest='repo', help="Output repository")
    parser.add_argument('-b', dest='branch', help="Branch")
    
    arguments = parser.parse_args(sys.argv[1:])
    filepart = arguments.output + '_{0:0d}.csv'
    splitter = CSVSplitter(arguments.input,filepart, arguments.chunk, True)
    splitter.split_file()
    print(splitter._length)
    if(arguments.repo):
        for f in splitter._files: 
            cmd = "pachctl put-file {0} {1} -c -f {2}".format(arguments.repo,arguments.branch,f)
            os.system(cmd)


