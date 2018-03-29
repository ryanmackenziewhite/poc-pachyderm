POC Project for Pachyderm

Define pipeline specifications
specs/

Source code for record linkage steps
record-linkage

Using a common Dockerfile to simplify testing, using, etc..

Record Linkage Example 1:
```
wget https://gitlab.k8s.cloud.statcan.ca/stcdatascience/poc-pachyderm/raw/master/scripts/demo1.sh
sh demo1.sh
cd demo1
source bin/activate
cd workdir
pachctl create-repo recordsA
pachctl create-repo recordsB
sh ./generate.sh EvolveModel 10 test recordsA recordsB master 1
```
Now update the pps files in record-linkage/specs
```
pachctl create-pipeline -f specs/cross.json
pachctl create-pipeline -f specs/join.json
```

Note, block_index.py has -v Validation option. We expect to get out what is put in.
Currently on large datasets 1 million, this is not the case. Needs to be debugged.

