import csv
import os
import sys
import argparse


def blocking(data,idx):
    if(data[-1][idx]==data[-2][idx]):
        return True
    else:
        return False


def process(in_path, out_path):
    print(in_path, out_path) 
    for dirpath, dirs, files in os.walk(in_path):
        data = []
        for file in files:
            path_to_file = os.path.join(dirpath,file)
            print(path_to_file)
            with open(path_to_file, 'r') as f:
                length = 0
                reader = csv.reader(f)
                for row in reader:
                    if length > 0: break
                    data.append(row)
                    length += 1
        print(data) 
        if(blocking(data,0)):
            
           for file in files:
                print(os.path.join(dirpath, file))
                os.symlink(os.path.join(dirpath, file),
                           os.path.join(out_path,file)) 

if __name__ == '__main__':
    #print(sys.argv)
    #print(sys.argv[0],sys.argv[1],sys.argv[2])
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', dest='inpath', action='store', required=True, help='Input path'  )
    parser.add_argument('-o', dest='outpath', action='store', required=True, help='Output path'  )
    arguments = parser.parse_args(sys.argv[1:])
    print(arguments.inpath, arguments.outpath)
    process(arguments.inpath,arguments.outpath)
    #process("/pfs/records","/pfs/out") 
    #process('/home/vagrant/Projects/adds/generate',
    #        '/home/vagrant/Projects/adds/link')
