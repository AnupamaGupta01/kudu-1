#!/bin/bash -e
# Calls tablet-decoder-eval-test with command line arguments:
#   $1: nrows
#   $2: cardinality
#   $3: strlen
#   $4: predicate upperbound
# Outputs are pushed to test-logs/tablet-decoder-eval-test.txt

LOG_DIR_NAME="build/release/test-logs"
BASE_DIR="../../.."
LOGDIR="$BASE_DIR/$LOG_DIR_NAME"
KUDU_ALLOW_SLOW_TESTS=true
log=$LOGDIR/tablet-decoder-eval-test.txt
KUDU_ALLOW_SLOW_TESTS=true ../../../build/release/bin/tablet-decoder-eval-test --nrows=$1 --cardinality=$2 --strlen=$3 --pred_upper=$4 --gtest_filter=* &> $log 
