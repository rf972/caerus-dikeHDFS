import time
import os
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

from pyspark.serializers import write_with_length
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
    executor = ThreadPoolExecutor(max_workers=len(columns))
    futures = list()
    for col in columns:
        fin = f.copy()
        pfin = pyarrow.parquet.ParquetFile(fin, metadata=pf.metadata)
        futures.append(executor.submit(read_col, pfin, rg, col))

    return [r.result().column(0) for r in futures]


class TpchSQL:
    def __init__(self, config):
        self.df = None
        self.ndp_data = None
        if config['use_ndp'] == 'True':
            self.remote_run(config)
        else:
            self.local_run(config)

    def remote_run(self, config):
        url = urllib.parse.urlparse(config['url'])
        conn = http.client.HTTPConnection(url.netloc)
        headers = {'Content-type': 'application/json'}
        conn.request("POST", config['url'], json.dumps(config), headers)
        response = conn.getresponse()
        self.ndp_data = response.read()
        conn.close()

    def local_run(self, config):
        f = WebHdfsFile(config['url'])
        pf = pyarrow.parquet.ParquetFile(f)
        tokens = sqlparse.parse(config['query'])[0].flatten()
        sql_columns = set([t.value for t in tokens if t.ttype in [sqlparse.tokens.Token.Name]])
        columns = [col for col in pf.schema_arrow.names if col in sql_columns]
        print(columns)
        rg = int(config['row_group'])
        tbl = pyarrow.Table.from_arrays(read_parallel(f, rg, columns), names=columns)
        self.df = duckdb.from_arrow_table(tbl).query("arrow", config['query']).fetchdf()
        print(f'Computed df {self.df.shape}')

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

        write_with_length(header.byteswap().newbyteorder().tobytes(), outfile)
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


if __name__ == '__main__':
    fname = '/tpch-test-parquet-1g/lineitem.parquet/' \
            'part-00000-badcef81-d816-44c1-b936-db91dae4c15f-c000.snappy.parquet'
    user = getpass.getuser()
    config = dict()
    config['use_ndp'] = 'True'
    config['row_group'] = '0'
    config['query'] = "SELECT l_partkey, l_extendedprice, l_discount FROM arrow WHERE l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'"
    config['url'] = f'http://dikehdfs:9860/{fname}?op=SELECTCONTENT&user.name={user}'

    start = time.time()
    tpchSQL = TpchSQL(config)
    end = time.time()
    print(f"Query time is: {end - start:.3f} secs")

