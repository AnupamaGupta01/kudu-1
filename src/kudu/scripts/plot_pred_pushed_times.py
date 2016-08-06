#!/usr/bin/python
import subprocess
import scipy
from plot_times import *

# grep through $log for the real times

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
        for i in range(nrepeats):
          subprocess.call(["./plot_pred_pushed_times.sh", str(nrows), str(cardinality), str(strlen), "21"])
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
        times.append(scipy.mean(t_list))
      # Plot the given set of times with different colors per cardinality
      ax.plot(cardinalities, times, c=str(strlen/max(strlen_list)))
  plt.show()
    
if __name__ == "__main__":
  main()
