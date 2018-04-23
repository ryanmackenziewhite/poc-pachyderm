#! /bin/sh
#
# generate.sh
# Copyright (C) 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.
#

if [[ $1 == 'help' || $1 == '-h' || $1 == '--help' ]]
then
    echo "Basic usage:"
    echo "./generate.sh <modelname> <Nevents> <datsetname>"
    echo "Pfs usage:"
    echo "./generate.sh <modelname> <Nevents <datasetname> <repo> <branch> <split>"
    echo "Pfs large dataset"
    echo "./generate.sh <modelname> <Nevents <datasetname> <repo1> <branch1> <repo2> <branch2>"
    exit 1
fi

if [[ "$#" -eq 3 ]]
then
    echo 'Generating data and write to local only'
    model=$1
    nevents=$2
    dsetname=$3
    split=$4
    echo -e "python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}"
    python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}
elif [[ "$#" -eq 5 ]]
then
    echo "Generating data and committing to pfs"
    model=$1
    nevents=$2
    dsetname=$3
    repo=$4
    branch=$5
    echo -e "python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}"
    python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}
    echo "Loop over output files and commit to pfs"
    shopt -s nullglob
    for f in *.csv
    do
        echo "pachctl put-file ${repo} ${branch} -c -f ${f}"
        pachctl put-file ${repo} ${branch} -c -f ${f}
    done    
elif [[ "$#" -eq 7 ]]
then
    echo "Generating data and committing to pfs"
    model=$1
    nevents=$2
    dsetname=$3
    repo1=$4
    branch1=$5
    repo2=$6
    branch2=$7
    echo -e "python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents} -f True"
    python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents} -f True
    echo "Loop over output files and commit to pfs"
    shopt -s nullglob
    for f in *.csv
    do
        echo "pachctl put-file ${repo1} ${branch1} -c -f ${f}"
        pachctl put-file ${repo1} ${branch1} -c -f ${f}
        echo "pachctl put-file ${repo2} ${branch2} -c -f ${f}"
        pachctl put-file ${repo2} ${branch2} -c -f ${f}
    done    
elif [[ "$#" -eq 6 ]]
then
    echo "Generating data and committing to pfs"
    model=$1
    nevents=$2
    dsetname=$3
    repo=$4
    branch=$5
    split=$6
    echo -e "python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}"
    python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}
    echo "Loop over output files and commit to pfs"
    shopt -s nullglob
    for f in *.csv
    do
        echo "pachctl put-file ${repo} ${branch} -c -f ${f} --split line --target-file-datums ${split}"
        pachctl put-file ${repo} ${branch} -c -f ${f} --split line --target-file-datums ${split}
    done    
    
elif [[ "$#" -eq 8 ]]
then
    echo "Generating data and committing to pfs with copy to two repos"
    model=$1
    nevents=$2
    dsetname=$3
    repo1=$4
    branch1=$5
    repo2=$6
    branch2=$7
    split=$8
    echo -e "python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}"
    python run_fakerstc.py -m ${model} -o ${dsetname} -n ${nevents}
    echo "Loop over output files and commit to pfs"
    shopt -s nullglob
    for f in *.csv
    do
        echo "pachctl put-file ${repo1} ${branch1} -c -f ${f} --split line --target-file-datums ${split}"
        echo "pachctl put-file ${repo2} ${branch2} -c -f ${f} --split line --target-file-datums ${split}"
        pachctl put-file ${repo1} ${branch1} -c -f ${f} --split line --target-file-datums ${split}
        pachctl put-file ${repo2} ${branch2} -c -f ${f} --split line --target-file-datums ${split}
    done    
else
    echo "No arguments provided"
    exit 1
fi

