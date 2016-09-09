#!/usr/bin/python
import subprocess
import re
import numpy as np
import matplotlib.pyplot as plt

BASE_DIR = "../../../"
LOG_DIR_NAME = "build/release/test-logs/"
LOGDIR = BASE_DIR+LOG_DIR_NAME
log = LOGDIR + "tablet-parameterized-test.txt"

def mean(int_list):
  if len(int_list) == 0:
    return 0
  return float(sum(int_list))/len(int_list)

def getMeanTime(nrows, cardn, strlen, nrepeats, pushdown, lower, upper):
  t_list = []
  subprocess.call(["./plot_pred_pushed_times.sh", str(nrows), str(cardn), str(strlen), str(lower), str(upper), str(nrepeats), str(pushdown)])
  log_file = open(log)
  for line in log_file:
    line = line.rstrip()
    print line
    if re.search("Time spent Filtering by string value:", line):
      t = re.findall("Filtering by string value: real ([0-9.]+)s\s+user", line)
      if len(t) > 0:
        t_list.append(float(t[0]))
  return mean(t_list)

def tune_dump(t_list, f, title):
  f.write(title + "\n")
  for t in t_list:
    f.write(str(t) + "\n")

def compare(f, getLower, getUpper, queryDescriptor):
  nrows = 10000000
  cardns_list = [100]
  strlen_list = [10]
  f.write("String lengths:" + str(strlen_list) + "\n")
  fig = plt.figure()
  nrepeats = 10
  for c, cardn in enumerate(cardns_list):
    ax = fig.add_subplot(1, len(cardns_list), 1+c)
    pushdown_times = []
    regular_times = []
    upper = getUpper(cardn)
    lower = getLower(cardn)
    for s, strlen in enumerate(strlen_list):
      t_pd = getMeanTime(nrows, cardn, strlen, nrepeats, "true", lower, upper)
      t_rg = getMeanTime(nrows, cardn, strlen, nrepeats, "false", lower, upper)
      pushdown_times.append(t_pd)
      regular_times.append(t_rg)

    tune_dump(pushdown_times, f, queryDescriptor + "_PUSHDOWN")
    tune_dump(regular_times, f, queryDescriptor + "_COPYEVAL")
    
if __name__ == "__main__":
  # Each of these tests should be handled with varying cardinalities
  #   Select single value: [0, upper), select 0
  #   Select a few values: [0, upper), select [0, upper/2)
  #   Select values that do not exist in dataset
  #   Select a predicate that captures all rows
  # FEW, EQUAL, ALL, EMPTY

  f = open('pushdown_benchmark.txt', "w")
  compare(f, lambda k: 0, lambda k: k/2, "SELECT_OVER_HALF_RANGE")   # l => 0, u => k/2
  compare(f, lambda k: k/2, lambda k: k/2+1, "SELECT_WHERE_EQUAL")   # l => k/2, u => k/2+1
  compare(f, lambda k: 0, lambda k: k, "SELECT_OVER_WHOLE_RANGE")    # l = 0, u = k
  compare(f, lambda k: k, lambda k: k+5, "SELECT_OUTSIDE_OF_RANGE")  # l => k, l => k+5
  f.close()
