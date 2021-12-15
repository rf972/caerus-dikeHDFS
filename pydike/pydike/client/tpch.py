import time
import os
import threading
import struct
import getpass
import http.client
import json
from concurrent.futures import ThreadPoolExecutor
import urllib.parse
import pyarrow
import pyarrow.parquet
import numpy
import duckdb
import sqlparse

from pydike.core.webhdfs import WebHdfsFile


class DataTypes:
    BOOLEAN = 0
    INT32 = 1
    INT64 = 2
    INT96 = 3
    FLOAT = 4
    DOUBLE = 5
    BYTE_ARRAY = 6
    FIXED_LEN_BYTE_ARRAY = 7

    type = {'int64': INT64, 'float64': DOUBLE, 'object': BYTE_ARRAY}


def read_col(pf, rg, col):
    return pf.read_row_group(rg, columns=[col])


def read_parallel(f, rg, columns):
    pf = pyarrow.parquet.ParquetFile(f)
    # Serial reading
    res = list()
    for col in columns:
        res.append(read_col(pf, rg, col).column(0))

    return res

    executor = ThreadPoolExecutor(max_workers=len(columns))
    futures = list()
    for col in columns:
        fin = f.copy()
        pfin = pyarrow.parquet.ParquetFile(fin, metadata=pf.metadata)
        futures.append(executor.submit(read_col, pfin, rg, col))

    return [r.result().column(0) for r in futures]


logging_lock = threading.Lock()

class TpchSQL:
    def __init__(self, config):
        self.df = None
        self.ndp_data = None
        self.config = config
        if config['use_ndp'] == 'True':
            self.remote_run()
        else:
            self.local_run()

    def log_message(self, msg):
        if self.config['verbose']:
            logging_lock.acquire()
            print(msg)
            logging_lock.release()

    def remote_run(self):
        url = urllib.parse.urlparse(self.config['url'])
        conn = http.client.HTTPConnection(url.netloc)
        headers = {'Content-type': 'application/json'}
        conn.request("POST", self.config['url'], json.dumps(self.config), headers)
        response = conn.getresponse()
        self.ndp_data = response.read()
        print(f'Received {len(self.ndp_data)} bytes')
        conn.close()

    def local_run(self):
        f = WebHdfsFile(self.config['url'])
        pf = pyarrow.parquet.ParquetFile(f)
        tokens = sqlparse.parse(self.config['query'])[0].flatten()
        sql_columns = set([t.value for t in tokens if t.ttype in [sqlparse.tokens.Token.Name]])
        columns = [col for col in pf.schema_arrow.names if col in sql_columns]
        self.log_message(columns)
        rg = int(self.config['row_group'])
        tbl = pyarrow.Table.from_arrays(read_parallel(f, rg, columns), names=columns)
        self.df = duckdb.from_arrow_table(tbl).query("arrow", self.config['query']).fetchdf()
        self.log_message(f'Computed df {self.df.shape}')

    def to_spark(self, outfile):
        if self.df is None:
            outfile.write(self.ndp_data)
            return

        header = numpy.empty(len(self.df.columns) + 1, numpy.int64)
        dtypes = [DataTypes.type[self.df.dtypes[c].name] for c in self.df.columns]
        header[0] = len(self.df.columns)
        i = 1
        for t in dtypes:
            header[i] = t
            i += 1

        buffer = header.byteswap().newbyteorder().tobytes()
        outfile.write(struct.pack("!i", len(buffer)))
        outfile.write(buffer)

        for col in self.df.columns:
            self.write_column(col, outfile)

    def write_column(self, column, outfile):
        data = self.df[column].to_numpy()
        header = None
        if data.dtype == 'object' and isinstance(data[0], str):
            s = data.astype(dtype=numpy.bytes_)
            l = numpy.char.str_len(s).astype(dtype=numpy.ubyte)
            fixed_len = numpy.all(l == l[0])
            data = s.tobytes()
            if fixed_len:
                data_type = DataTypes.FIXED_LEN_BYTE_ARRAY
                header = numpy.array([data_type, l[0], len(data), 0], numpy.int32)
            else:
                data_type = DataTypes.BYTE_ARRAY
                h = numpy.array([data_type, 0, len(l), 0], numpy.int32)
                outfile.write(h.byteswap().newbyteorder().tobytes())
                outfile.write(l.tobytes())
                header = numpy.array([data_type, 0, len(data), 0], numpy.int32)

        else:  # Binary type
            data = data.byteswap().newbyteorder().tobytes()
            data_type = DataTypes.type[self.df.dtypes[column].name]
            header = numpy.array([data_type, 0, len(data), 0], numpy.int32)

        outfile.write(header.byteswap().newbyteorder().tobytes())
        outfile.write(data)

def run_test(row_group):
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'
    user = getpass.getuser()
    config = dict()
    config['use_ndp'] = 'True'
    config['row_group'] = str(row_group)
    config['query'] = "SELECT l_partkey, l_extendedprice, l_discount FROM arrow WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'"
    config['url'] = f'http://dikehdfs:9860/{fname}?op=SELECTCONTENT&user.name={user}'

    return TpchSQL(config)


if __name__ == '__main__':
    rg_count = 3
    start = time.time()
    executor = ThreadPoolExecutor(max_workers=rg_count)
    futures = list()
    for i in range(0, rg_count):
        futures.append(executor.submit(run_test, i))

    res = [f.result() for f in futures]
    end = time.time()
    print(f"Query time is: {end - start:.3f} secs")

