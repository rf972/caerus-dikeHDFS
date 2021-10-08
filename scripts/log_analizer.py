import os
import sys
from matplotlib import pyplot as plt

def analyze(fname):
    f = open(fname, 'r')
    time_list = []
    ndp_delta = []
    actual_runtime = []
    t_start = 0.0
    t_end = 0.0

    for line in f:
        if "DN Start" in line:
            ts =  line.split(" ")[1].split(":")
            t_start = float(ts[0]) * 3600 + float(ts[1]) * 60 + float(ts[2]) + float(ts[3])/1000.0
            time_list.append(t_start)

        if "Actual run_time" in line:
            ts = line.split(" ")[2]
            actual_runtime.append(float(ts))

        if "DN End" in line:
            ts =  line.split(" ")[1].split(":")
            t_end = float(ts[0]) * 3600 + float(ts[1]) * 60 + float(ts[2]) + float(ts[3])/1000.0
            #print("{} {}".format(t_start, t_end))
            ndp_delta.append(t_end - t_start)


    f.close()
    time_delta = []
    total_delta = 0
    total_ndp = 0
    total_actual = 0
    for i in range(1, len(time_list)):
        time_delta.append(time_list[i] - time_list[i-1])
        total_delta += time_list[i] - time_list[i-1]
        total_ndp += ndp_delta[i -1]
        total_actual += actual_runtime[i -1]

    plt.plot(time_delta[:], 'ro')
    plt.plot(ndp_delta[:], 'g+')
    plt.plot(actual_runtime[:], 'b.')
    plt.savefig(fname + ".timing.png")
    title = "Total delta {} NDP {} Actual {}".format(int(total_delta), int(total_ndp), int(total_actual))
    plt.title(title)
    plt.show()


if __name__ == "__main__":
    print("Analyzing {}".format(sys.argv[1]))
    analyze(sys.argv[1])
