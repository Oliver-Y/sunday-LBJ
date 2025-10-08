#!/bin/bash


python3 bronze.py \
  --url "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/bulk-data/opinions-2025-09-04.csv.bz2" \
  --out-dir "data/bronze/opinions/2025-10-05" \
  --rows-per-shard 1000000 \
  -v

