#!/bin/bash -e
# Calls tablet-parameterized-test with command line arguments:
#   $1: nrows
#   $2: cardinality
#   $3: strlen
#   $4: predicate upperbound
# Outputs are pushed to test-logs/tablet-parameterized-test.txt

LOG_DIR_NAME="build/release/test-logs"
BASE_DIR="../../.."
LOGDIR="$BASE_DIR/$LOG_DIR_NAME"
log=$LOGDIR/tablet-parameterized-test.txt
../../../build/release/bin/tablet-parameterized-test\
           --tablet_param_test_nrows=$1\
           --tablet_param_test_cardinality=$2\
           --tablet_param_test_strlen=$3\
           --tablet_param_test_lower=$4\
           --tablet_param_test_upper=$5\
           --tablet_param_test_nrepeats=$6\
           --materializing_iterator_decoder_eval=$7\
           --gtest_filter=* &> $log 
