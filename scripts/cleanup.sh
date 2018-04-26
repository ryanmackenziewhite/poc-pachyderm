#! /bin/sh
#
# cleanup.sh
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

pachctl delete-pipeline ${STEP5}
pachctl delete-pipeline ${STEP4}
pachctl delete-pipeline ${STEP3}
pachctl delete-pipeline ${STEP2}
pachctl delete-pipeline ${STEP1}
pachctl delete-repo ${INPUT1}
pachctl delete-repo ${INPUT2}
