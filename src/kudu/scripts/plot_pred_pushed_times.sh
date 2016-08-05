#!/bin/bash -e
LOG_DIR_NAME="build/release/test-logs"
BASE_DIR="../../.."
LOGDIR="$BASE_DIR/$LOG_DIR_NAME"
KUDU_ALLOW_SLOW_TESTS=true
log=$LOGDIR/tablet-decoder-eval-test.txt
KUDU_ALLOW_SLOW_TESTS=true ../../../build/release/bin/tablet-decoder-eval-test --nrows=$1 --cardinality=$2 --strlen=$3 --pred_upper=$4 --gtest_filter=* &> $log 
