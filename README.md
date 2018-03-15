POC Project for Pachyderm

Define pipeline specifications
specs/

Source code for record linkage steps
record-linkage

Using a common Dockerfile to simplify testing, using, etc..

Record Linkage Example 1:
```
mkdir test
cd test
git clone https://gitlab.k8s.cloud.statcan.ca/stcdatascience/adds.git
git clone https://gitlab.k8s.cloud.statcan.ca/stcdatascience/poc-pachyderm.git
mkdir workdir
python3 -m venv /<path_to>/test
source bin/activate
cd poc-pachyderm
pip install --upgrade pip
pip install -r requirements.txt
pip install -r extras.txt
cd ../adds
python setup.py install 
cd ../workdir
ln -s ../adds/adds/cli/run_fakerstc.py run_fakerstc.py
ln -s ../poc-pachyderm/scripts/generate.sh ./generate.sh
python run_fakerstc.py --help
sh generate.sh --help
cd ../poc-pachyderm
docker build -t poc-test:v0.1
pachctl create-repo testA
pachctl create-repo testB
sh ./generate.sh EvolveModel 10 test testA testB master 1
```
Now update the pps files in record-linkage/specs
```
pachctl create-pipeline -f specs/cross.json
pachctl create-pipeline -f specs/join.json
```
