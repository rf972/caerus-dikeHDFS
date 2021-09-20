import subprocess

nodes = [
    "172.169.1.10",
    "172.169.1.20",
    "172.169.1.30",
    "172.169.1.12",
    "172.169.1.40",
    "172.169.1.60",
    "172.170.1.60",
]

def check_ping(node_name):        
    response = subprocess.call("timeout 0.5 ping -c 1 " + node_name, shell=True,  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)    
    if response == 0:
        print( node_name + " Active")
    else:
        print( node_name + " Error")

for node in nodes:
    check_ping(node)

