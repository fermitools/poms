#!/bin/bash

PYTHONPATH="../../job_broker/poms_client/python:$PYTHONPATH"
PATH="../../job_broker/poms_client/bin:$PATH"

ds=`date +%s`
cid=74 
tid=`get_task_id_for --campaign=$cid --user=$USER --experiment=samdev --test=1`
njobs=2048

for stat in 1 2 4
do
   echo "writing condor_q_batch_${njobs}_${cid}_stat_${stat}_out"
   i=0
   while [ $i -lt 20000 ] 
   do
      cat <<EOF
Args="--fake-job 1 --nosuch";CONDOR_EXEC=/tmp;DAGMANJOBID=;EnteredCurrentStatus=$ds;EXPERIMENT=samdev;GLIDEIN_SITE=fakesite;GRID_USER=mengel;HoldReason=;IFDH_BASE_URI=http://samweb.fnal.gov:8480/sam/samdev/api;IFDH_DEBUG=1;JOBSTATUS=$stat;JOBSUBJOBID=$ds.$i@fakebatch.fnal.gov;NumRestarts=0;POMS_CAMPAIGN_ID=$cid;POMS_TASK_ID=$tid;PROCESS=$i;RemoteUserCpu=$((status-1)).000000;RemoteWallClockTime=$((status-1)).000000;SAM_PROJECT_NAME=fake-project-$ds SAM_STATION=samdev;xxx=$st
EOF
      i=$((i + 1))
   done > condor_q_batch_${njobs}_${cid}_stat_${stat}_out
done
