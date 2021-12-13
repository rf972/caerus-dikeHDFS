
def get_memory_usage_mb():
    """ Memory usage in MB """

    with open('/proc/self/status') as f:
        mem_usage = f.read().split('VmRSS:')[1].split('\n')[0][:-3]

    return int(mem_usage.strip()) // 1024
