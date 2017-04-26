#!/bin/bash
impala-shell -k -i cs108 -f feature_dense_merge_online.sql
impala-shell -k -i cs108 -f feature_sparse_merge_online.sql