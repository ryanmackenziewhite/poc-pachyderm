#! /bin/sh
#
# autogen_pipeline.sh
# Copyright (C) 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.
#

MODEL=$1
DSETTAG=$2
EVT1='_evt_set1_'
EVT2='_evt_set2_'
GEN1='_gen_set1_'
GEN2='_gen_set2_'
FEATS='_features_'
MERGE='_merge_'
VALID='_valid_'

INPUT1=$DSETTAG$EVT1
INPUT2=$DSETTAG$EVT2
STEP1=$DSETTAG$GEN1
STEP2=$DSETTAG$GEN2
STEP3=$DSETTAG$FEATS
STEP4=$STEP1$MERGE
STEP5=$STEP2$MERGE
STEP6=$STEP4$STEP5$VALID

echo "$STEP1 $STEP2 $STEP3 $STEP4 $STEP5 $STEP6"
python run_fakerstc.py \
    -g True \
    -m $1 \
    -n 1000 \
    -f True \
    -j 10 \
    -r /pfs/out/$DSETTAG \
    -s 5236

pachctl create-repo ${INPUT1}
pachctl create-repo ${INPUT2}
shopt -s nullglob
for f in *.json
do
    echo "pachctl put-file ${INPUT1} master -c -f ${f}"
    echo "pachctl put-file ${INPUT2} master -c -f ${f}"
    pachctl put-file ${INPUT1} master -c -f ${f}
    pachctl put-file ${INPUT2} master -c -f ${f}
    #rm ${f}
done    


python pfsgenerator.py \
    -p "${STEP1}" \
    -i "atom" \
    -r "${INPUT1}" \
    -g "/*" \
    -d "adds:v01" \
    -c "sh" \
    -l "constant" "4" \
    -t "sh /adds/scripts/autogen.sh /pfs/${INPUT1}" 

pachctl create-pipeline -f ${STEP1}'.json'

python pfsgenerator.py \
    -p "${STEP2}" \
    -i "atom" \
    -r "${INPUT2}" \
    -g "/*" \
    -d "adds:v01" \
    -c "sh" \
    -l "constant" "4" \
    -t "sh /adds/scripts/autogen.sh /pfs/${INPUT2}" 

pachctl create-pipeline -f ${STEP2}'.json'

python pfsgenerator.py \
    -p "${STEP3}" \
    -i "cross" \
    -r "${STEP1}" \
    -s "${STEP2}" \
    -g "/*" \
    -d "poc-test:v02" \
    -c ""python3" "/record-linkage/block_index.py" "-a" "/pfs/${STEP1}" "-b" "/pfs/${STEP2}" "-o" "/pfs/out/" "-v" "True" "-k" "SIN"" \
    -l "constant" "4"

pachctl create-pipeline -f ${STEP3}'.json'

python pfsgenerator.py \
    -p "${STEP4}" \
    -i "atom" \
    -r "${STEP3}" \
    -g "/*" \
    -d "poc-test:v02" \
    -l "constant" "1" \
    -c ""python3" "/record-linkage/merge_output.py" "-a" "/pfs/${STEP3}" "-na" "output.valid.csv" "-o" "/pfs/out/"" 

pachctl create-pipeline -f ${STEP4}'.json'

python pfsgenerator.py \
    -p "${STEP5}" \
    -i "atom" \
    -r "${STEP1}" \
    -g "/*" \
    -d "poc-test:v02" \
    -c ""python3" "/record-linkage/merge_input.py" "-a" "/pfs/${STEP1}" "-o" "/pfs/out/"" \
    -l "constant" "1"
pachctl create-pipeline -f ${STEP5}'.json'

python pfsgenerator.py \
    -p "${STEP6}" \
    -i "cross" \
    -r "${STEP4}" \
    -s "${STEP5}" \
    -g "/*" \
    -d "poc-test:v02" \
    -c ""python3" "/record-linkage/validate2.py" "-b" "/pfs/${STEP5}" "-a" "/pfs/${STEP4}"" \
    -l "constant" "1"

pachctl create-pipeline -f ${STEP6}'.json'



    


