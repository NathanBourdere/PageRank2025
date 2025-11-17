#!/bin/bash
REGION="europe-west1"
CLUSTER="$1"        # e.g. pagerank-2nodes
BUCKET="pagerank2025-nathan-bucket"
INPUT="gs://$BUCKET/data/sample10pct/*"

# Submit RDD job
gcloud dataproc jobs submit pyspark \
  --region=$REGION \
  --cluster=$CLUSTER \
  src/pagerank_rdd.py -- $INPUT 10

# Submit DataFrame job
gcloud dataproc jobs submit pyspark \
  --region=$REGION \
  --cluster=$CLUSTER \
  src/pagerank_df.py -- $INPUT 10
