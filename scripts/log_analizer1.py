import os
import sys
from matplotlib import pyplot as plt
import json

def plot_records(fname, records):
    ndp_delta = []
    actual_runtime = []
    time_delta = []
    total_delta = 0
    total_ndp = 0
    total_actual = 0

    for i in range(1, len(records)):        
        time_delta.append(records[i]["DN Start"] - records[i-1]["DN Start"])
        total_delta += time_delta[-1]

        ndp_delta.append(records[i-1]["DN End"] - records[i-1]["DN Start"])
        total_ndp += ndp_delta[-1]

        actual_runtime.append(records[i-1]["Actual run_time"])
        total_actual += actual_runtime[-1]

    b = []
    for r in records:
        b.append((r["Actual run_time"] * 50000 / r["Records"]))

    rc = []
    for r in records:
        rc.append(r["Records"] / 50000)

    plt.figure()
    #plt.plot(time_delta[:], 'ro')
    #plt.plot(ndp_delta[:], 'g+')
    plt.plot(actual_runtime[:], 'b.')
    plt.plot(b[:], 'y.')
    plt.plot(rc[:], 'r.')
    plt.savefig(fname + ".timing.png")
    title = "Total delta {} NDP {} Actual {}".format(int(total_delta), int(total_ndp), int(total_actual))
    plt.title(title)
    plt.show()


def analyze(fname):
    f = open(fname, 'r')
    record = {}
    records = []

    for line in f:
        dn_started = False
        if "DN Start" in line:
            ts =  line.split(" ")[1].split(":")
            t_start = float(ts[0]) * 3600 + float(ts[1]) * 60 + float(ts[2]) + float(ts[3])/1000.0
            record["DN Start"] = t_start
            dn_started = True

        if "Actual run_time" in line:
            ts = line.split(" ")[2]            
            record["Actual run_time"] = float(ts)

        if "Configuration.DAG = " in line:
            jstr = line.split("Configuration.DAG = ")[1]            
            jDict = json.loads(jstr)
            #print(jDict['NodeArray'][0]['File'])
            record["File"] = jDict['NodeArray'][0]['File']

        if "Records" in line:
            rs = line.split(" ")[1]
            record["Records"] = int(rs)

        if "DN End" in line:
            ts =  line.split(" ")[1].split(":")
            t_end = float(ts[0]) * 3600 + float(ts[1]) * 60 + float(ts[2]) + float(ts[3])/1000.0
            record["DN End"] = t_end
            dn_started = False
            records.append(record)
            record = {}

    f.close()
    

    lineitem = []
    for r in records:
        if "lineitem" in r["File"]:
            lineitem.append(r)

    part = []
    for r in records:
        if "part.parquet" in r["File"]:
            part.append(r)

    plot_records(fname, lineitem)



if __name__ == "__main__":
    print("Analyzing {}".format(sys.argv[1]))
    analyze(sys.argv[1])
