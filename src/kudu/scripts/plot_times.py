#!/usr/bin/python
import re
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

def plot_times():
  nrows = [1000000]
  cardinalities = [2, 4, 6, 8, 10, 15, 20, 30, 40, 80, 100, 200, 400, 600, 800, 1000]
  strlens = [8, 24, 40, 56, 72]
  BASE_DIR = "../../../"
  LOG_DIR_NAME = "build/release/test-logs/"
  LOGDIR = BASE_DIR+LOG_DIR_NAME
  log = LOGDIR + "tablet-decoder-eval-test.txt"

  xs = []
  ys = []
  ts = []
  reals = []
  print log
  log_file = open(log)

  # Isolate the real times in the file
  for line in log_file:
    line = line.rstrip()
    if re.search("Time spent Filtering by string value:", line):
      t = re.findall('Filtering by stirng value: real ([0-9.]+)s\s+user', line)
      if len(t) > 0:
        reals.append(float(t[0]))

  print reals
  print len(reals)
  i = 0
  csv_str = ""
  fig = plt.figure()
  for rows in nrows:
    ax = fig.add_subplot(111, projection='3d')
    for cardinality in cardinalities:
      for c, strlen in zip(['r', 'g', 'b', 'y', 'r'], strlens):
        xs.append(cardinality)
        ys.append(strlen)
        ts.append(reals[i])
        cs = [c]*len(xs)
        csv_str += str(cardinality)+", "+str(strlen)+", "+str(reals[i])+"\n"
        cs[0] = 'c'
        ax.bar(xs, ts, ys, zdir='y', color=cs, alpha=0.8)
        i += 1
      ax.set_xlabel("Cardinality")
      ax.set_ylabel("String Length")
      ax.set_zlabel("Time (s)")
      # ax.set_zlim(0, 0.01)
      # ax.set_xlim(0, 20)
      xs = []
      ys = []
      ts = []
    fig.savefig("out"+str(rows)+"rows.png")

    out_file = open("./out"+str(rows)+"rows.csv", "wb")
    out_file.write(csv_str)

if __name__ == "__main__":
  plot_times()
