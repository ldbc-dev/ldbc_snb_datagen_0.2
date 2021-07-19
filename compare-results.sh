#!/bin/bash

export SF=1

LDBC_DATA_DIR_TIMESTAMP=$(aws s3 ls s3://ldbc-snb-datagen-bi-2021-07/results/sf${SF}-test/runs/ | grep -o '[0-9][0-9]*_[0-9][0-9]*' | head -n 1)
aws s3 cp --recursive s3://ldbc-snb-datagen-bi-2021-07/results/sf${SF}-test/runs/${LDBC_DATA_DIR_TIMESTAMP}/social_network/csv/raw/composite-merged-fk sf${SF}-emr
aws s3 rm --recursive s3://ldbc-snb-datagen-bi-2021-07/results/sf${SF}-test/runs/${LDBC_DATA_DIR_TIMESTAMP}

export ENTITY=Person
echo "===== ${ENTITY} ====="
tail -qn +2 sf${SF}-local/csv/raw/*/*/${ENTITY}/*.csv | wc -l
tail -qn +2 sf${SF}-emr/*/${ENTITY}/part_*.csv | wc -l

export ENTITY=Forum
echo "===== ${ENTITY} ====="
tail -qn +2 sf${SF}-local/csv/raw/*/*/${ENTITY}/*.csv | wc -l
tail -qn +2 sf${SF}-emr/*/${ENTITY}/part_*.csv | wc -l

echo "---- local ----"
tail -qn +2 sf${SF}-local/csv/raw/*/*/${ENTITY}/*.csv | sort | head
echo "----- EMR -----"
tail -qn +2 sf${SF}-emr/*/${ENTITY}/part_*.csv | sort | head
