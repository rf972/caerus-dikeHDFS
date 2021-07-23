import os
import sys
from matplotlib import pyplot as plt

def analyze(fname):
    f = open(fname, 'r')
    time_list = []
    ndp_delta = []
    t_start = 0.0
    t_end = 0.0

    for line in f:
        if "DN Start" in line:
            ts =  line.split(" ")[1].split(":")
            t_start = float(ts[0]) * 3600 + float(ts[1]) * 60 + float(ts[2]) + float(ts[3])/1000.0
            time_list.append(t_start)

        if "DN End" in line:
            ts =  line.split(" ")[1].split(":")
            t_end = float(ts[0]) * 3600 + float(ts[1]) * 60 + float(ts[2]) + float(ts[3])/1000.0
            #print("{} {}".format(t_start, t_end))
            ndp_delta.append(t_end - t_start)


    f.close()
    time_delta = []
    for i in range(1, len(time_list)):
        time_delta.append(time_list[i] - time_list[i-1])

    plt.plot(time_delta[:])
    plt.plot(ndp_delta[:])
    plt.savefig(fname + ".timing.png")
    plt.show()


if __name__ == "__main__":
    print("Analyzing {}".format(sys.argv[1]))
    analyze(sys.argv[1])
