#!/usr/bin/python
import subprocess
import re
import numpy as np
import matplotlib.pyplot as plt

BASE_DIR = "../../../"
LOG_DIR_NAME = "build/release/test-logs/"
LOGDIR = BASE_DIR+LOG_DIR_NAME
log = LOGDIR + "tablet-decoder-eval-test.txt"
def mean(int_list):
  if len(int_list) == 0:
    return 0
  return float(sum(int_list))/len(int_list)

def getMeanTime(nrows, cardn, strlen, nrepeats, pushdown):
  t_list = []
  subprocess.call(["./plot_pred_pushed_times.sh", str(nrows), str(cardn), str(strlen), "21", str(nrepeats), str(pushdown)])
  log_file = open(log)
  for line in log_file:
    line = line.rstrip()
    print line
    if re.search("Time spent Filtering by string value:", line):
      t = re.findall("Filtering by string value: real ([0-9.]+)s\s+user", line)
      if len(t) > 0:
        t_list.append(float(t[0]))
  return mean(t_list)

def compare():
  nrows = 1000000
  cardns_list = [10, 100, 5000, 60000]
  # cardns_list = [9, 99, 333, 1832]
  strlen_list = [50, 100, 150]
  fig = plt.figure()
  nrepeats = 10
  width = 0.3
  ylim = 0
  for c, cardn in enumerate(cardns_list):
    ax = fig.add_subplot(1, len(cardns_list), 1+c)
    cardinalities = []
    strlens = []
    pushdown_times = []
    regular_times = []
    for s, strlen in enumerate(strlen_list):
      t_pd = getMeanTime(nrows, cardn, strlen, nrepeats, "true")
      t_rg = getMeanTime(nrows, cardn, strlen, nrepeats, "false")
      cardinalities.append(cardn)
      strlens.append(strlen)
      pushdown_times.append(t_pd)
      regular_times.append(t_rg)
    inds = np.arange(len(strlen_list))
    if c == 0:
      ax.set_ylabel("Real time(s)")
    if ylim == 0:
      ylim = max(regular_times)*1.25
    pd = ax.bar(inds+0.5*width, np.array(pushdown_times)+0, width, color='dodgerblue', edgecolor='dodgerblue')
    rg = ax.bar(inds+1.5*width+0.01, np.array(regular_times)+0, width, color='skyblue', edgecolor='skyblue')
    ax.set_ylim(0, ylim)
    ax.set_xticks(inds+width)
    ax.set_title("Cardinality: "+str(cardn))
    ax.set_xticklabels(strlen_list)
    if c == len(cardns_list)-1:
      ax.legend((pd[0], rg[0]), ('Pushdown', 'Normal Eval'))

  plt.show()

def main():
  nrows_list = [1000, 100000, 1000000]
  cardinalities_list = [5, 10, 100, 300, 1000]
  strlen_list = [10,50,100]
  BASE_DIR = "../../../"
  LOG_DIR_NAME = "build/release/test-logs/"
  LOGDIR = BASE_DIR+LOG_DIR_NAME
  log = LOGDIR + "tablet-decoder-eval-test.txt"
  
  nrepeats = 5
  
  fig = plt.figure()
  for n, nrows in enumerate(nrows_list):
    ax = fig.add_subplot(1,len(nrows_list),1+n)
    xs = []
    ys = []
    ts = []
    for s, strlen in enumerate(strlen_list):
      cardinalities = []
      times = []
      for cardinality in cardinalities_list:
        t_list = []
        subprocess.call(["./plot_pred_pushed_times.sh", str(nrows), str(cardinality), str(strlen), "21", str(nrepeats), "true"])
        log_file = open(log)
        for line in log_file:
          line = line.rstrip()
          if re.search("Time spent Filtering by string value:", line):
            t = re.findall("Filtering by string value: real ([0-9.]+)s\s+user", line)
            if len(t) > 0:
              t_list.append(float(t[0]))
        # Add the data for a single set of parameters (nrepeats runs)
        cardinalities.append(cardinality)
        xs.append(strlen)
        times.append(mean(t_list))
      # Plot the given set of times with different colors per cardinality
      ax.plot(cardinalities, times, c=str(strlen/max(strlen_list)))
  plt.show()
    
if __name__ == "__main__":
  compare()