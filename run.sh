#!/bin/bash

mvn compile exec:java \
    -Dexec.mainClass=PopularUsNames.class \
    -Pdataflow-runner \
    -Dexec.args=" \
    --project=test-bq-331608 \
    --gcpTempLocation=gs://test_bq-bucket/temp/ \
    --stagingLocation=gs://test_bq-bucket/staging/ \
    --runner=DataflowRunner \
    --inputFile=gs://test_bq-bucket/AVRO/usnames100.avro \
    --outputDir=gs://test_bq-bucket/CSV/output/"

