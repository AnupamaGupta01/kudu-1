#!/bin/bash -xe
NUM_SAMPLES=10
LOG_DIR_NAME="build/release/test-logs"
BASE_DIR="../../.."
LOGDIR="$BASE_DIR/$LOG_DIR_NAME"
KUDU_ALLOW_SLOW_TESTS=true
log=$LOGDIR/tablet-decoder-eval-test.txt
KUDU_ALLOW_SLOW_TESTS=true ../../../build/release/bin/tablet-decoder-eval-test \
  --gtest_filter=* &> $log
