import os
import sys
from matplotlib import pyplot as plt

def analyze(fname):
    f = open(fname, 'r')
    sql_list = set()

    for line in f:
        if "Configuration.Query = " in line and "ReadParam:" not in line:
            q =  line.split("Configuration.Query = ")[1].split('\n')[0]          
            sql_list.add(q)

    f.close()

    for sql in sql_list:
        print(sql)

if __name__ == "__main__":
    print("Analyzing {}".format(sys.argv[1]))
    analyze(sys.argv[1])
