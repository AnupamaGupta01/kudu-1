#!/bin/bash -xe

NUM_SAMPLES=${NUM_SAMPLES:-10}
LOGDIR="$BASE_DIR/$LOG_DIR_NAME"
LOG_DIR_NAME=build/latest/bench-logs
for i in $(seq 1 $NUM_SAMPLES); 
  log=$LOGDIR/${PREDPUSHED_TEST}$i
  KUDU_ALLOW_SLOW_TESTS=true ./build/latest/bin/tablet-decoder-eval-test \
    --gtest_filter=* &> $log.log
  real=`grep "Time spent Filtering by string value:" $log | ./parse_real_out.sh`
done 
