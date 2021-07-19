#!/bin/bash

export SF=1

./tools/build.sh

# EMR run
rm -rf sf${SF}-emr
aws s3 cp target/ldbc_snb_datagen_2.11_spark2.4-0.4.0-SNAPSHOT-jar-with-dependencies.jar s3://${BUCKET_NAME}/jars/ldbc_snb_datagen_2.11_spark2.4-0.4.0-SNAPSHOT-jar-with-dependencies.jar
./tools/emr/submit_datagen_job.py --bucket ${BUCKET_NAME} --az us-east-2a sf${SF}-test ${SF} -- --format csv --mode raw

# local run
rm -rf sf${SF}-local
./tools/run.py ./target/ldbc_snb_datagen_${PLATFORM_VERSION}-${DATAGEN_VERSION}.jar -- --format csv --scale-factor ${SF} --mode raw --output-dir sf${SF}-local
