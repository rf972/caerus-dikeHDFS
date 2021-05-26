import os
import sys
from matplotlib import pyplot as plt

def analyze(fname):
    f = open(fname, 'r')
    time_list = []
    for line in f:        
        if "DN Start" in line:
            ts =  line.split(" ")[1].split(":")
            t = int(ts[0]) * 3600 + int(ts[1]) * 60 + int(ts[2])
            time_list.append(t)
    
    f.close()
    time_delta = []
    for i in range(1, len(time_list)):
        time_delta.append(time_list[i] - time_list[i-1])

    plt.plot(time_delta[:])
    plt.savefig(fname + ".timing.png")
    plt.show()


if __name__ == "__main__":
    print("Analyzing {}".format(sys.argv[1]))
    analyze(sys.argv[1])
