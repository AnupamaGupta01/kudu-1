#!/usr/bin/python
import subprocess
from plot_times import *

# grep through $log for the real times

def main():
  subprocess.call(["./plot_pred_pushed_times.sh"])
  plot_times()

    
if __name__ == "__main__":
  main()
