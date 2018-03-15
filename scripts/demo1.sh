#! /bin/sh
#
# demo1.sh
# Copyright (C) 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.
#

mkdir demo1
cd demo1
wget https://gitlab.k8s.cloud.statcan.ca/stcdatascience/poc-pachyderm/repository/master/archive.tar.gz -O ./poc-pachyderm.tar.gz
wget https://gitlab.k8s.cloud.statcan.ca/stcdatascience/adds/repository/master/archive.tar.gz -O ./adds.tar.gz
mkdir poc-pachyderm
mkdir adds
tar -xzvf poc-pachyderm.tar.gz -C poc-pachyderm --strip-components=1
tar -xzvf adds.tar.gz -C adds --strip-components=1
mkdir workdir
python3 -m venv $(pwd)
source bin/activate
pip install --upgrade pip
pip install -r poc-pachyderm/requirements.txt
pip install -r poc-pachyderm/extras.txt
cd adds
python setup.py install
cd ../workdir
ln -s ../adds/adds/cli/run_fakerstc.py ./run_fakerstc.py
ln -s ../poc-pachyderm/scripts/generate.sh ./generate.sh
ln -s ../poc-pachyderm/specs ./specs
python run_fakerstc.py --help
sh ./generate.sh --help
cd ../poc-pachyderm
docker build -t poc-test:v0.1 .
deactivate
