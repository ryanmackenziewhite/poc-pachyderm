#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
File utility for retrieving, creating, naming, etc.
files in pachyderm pods
Uses tempfile to create unique names -- needs to be better solution
"""
import os
import tempfile
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger('FileUtil')

class FileUtil(object):

    def __init__(self, repo):
        self._repo = repo
        self._filenames = []
        self._filepaths = []
        self._files = {}
        self._linkfiles = {}
    
    @property
    def repo(self):
        return self._repo

    @repo.setter
    def repo(self, repo):
        self._repo = repo
    
    @property
    def datums(self):
        return self._files
    
    def remove(self, key):
        '''
        Mutates file list, removing filtered files
        '''
        self._files.pop(key)

    def mkdir(self, prefix='/pfs/out'):
        '''
        Wrapper for tempfile.mkdtemp
        place-holder for improved directory management
        '''
        tmpname = tempfile.mkdtemp(prefix='/pfs/out/')
        return tmpname
    
    def link(self,tmpdir=True):
        '''
        Create output files from input file list
        '''
        if tmpdir is True:
            outdir = self.mkdir()
        else:
            outdir = '/pfs/out'
        
        for key in self._files:
            os.symlink(self._files[key], os.path.join(outdir, key))
            

    def update_files(self, name, path):
        '''
        Do we assume unique file names?
        '''
        if name in self._files.keys():
            log.error('Warning, file already exists')
        else:
            _path = os.path.normpath(os.path.abspath(path)) 
            log.debug('%s',_path)
            self._files[name] = _path

    def walk(self):
        log.info('FileUtil:walk') 
        for root, dirs, files in os.walk(self.repo):
            for name in files:
                path_to_file = os.path.join(root, name)
                log.debug('%s',path_to_file)
                self._filenames.append(name)
                self._filepaths.append(path_to_file)
                self.update_files(name, path_to_file)

            for name in dirs:
                path_to_dir = os.path.join(root, name)
                log.debug('%s',path_to_dir)

    def show(self):
        print("FileUtil:show")
        print("Filenames: ")
        print(self._filenames)
        print("Filepaths")
        print(self._filepaths)
        print(self._files)
        print('----------------------------')

     
