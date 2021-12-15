import os
import urllib.parse
import http.client
import json


class WebHdfsFile(object):
    def __init__(self, name, user=None, buffer_size=(128 << 10), size=None, data_url=None, data_req=None):
        self.name = name
        self.user = user
        self.buffer_size = buffer_size
        self.mode = 'rb'
        self.closed = False
        self.offset = 0
        self.read_stats = []
        self.read_bytes = 0
        self.verbose = False

        if size is None:  # Support for copy constructor
            self.url = urllib.parse.urlparse(self.name)
            if self.user is None:
                for q in self.url.query.split('&'):
                    if 'user.name=' in q:
                        self.user = q.split('user.name=')[1]

            self.conn = http.client.HTTPConnection(self.url.netloc)
            req = f'/webhdfs/v1/{self.url.path}?op=GETFILESTATUS&user.name={self.user}'
            self.conn.request("GET", req)
            resp = self.conn.getresponse()
            resp_data = resp.read()
            file_status = json.loads(resp_data)['FileStatus']
            self.size = file_status['length']
        else:
            self.size = size

        if data_req is None or data_url is None:  # Support for copy constructor
            req = f'/webhdfs/v1{self.url.path}?op=OPEN&user.name={self.user}&buffersize={self.buffer_size}'
            self.conn.request("GET", req)
            resp = self.conn.getresponse()
            self.data_url = urllib.parse.urlparse(resp.headers['Location'])
            self.conn.close()

            query = self.data_url.query.split('&offset=')[0]
            self.data_req = f'{self.data_url.path}?{query}'
        else:
            self.data_req = data_req
            self.data_url = data_url

    def copy(self):  # Copy constructor
        f = WebHdfsFile(self.name, self.user, self.buffer_size, self.size, self.data_url, self.data_req)
        return f

    def seek(self, offset, whence=0):
        #print("Attempt to seek {} from {}".format(offset, whence))
        if whence == os.SEEK_SET:
            self.offset = offset
        elif whence == os.SEEK_CUR:
            self.offset += offset
        elif whence == os.SEEK_END:
            self.offset = self.size + offset

        return self.offset

    def read(self, length=-1):
        if self.verbose:
            print(f"Attempt to read from {self.offset} len {length}")
        pos = self.offset
        if length == -1:
            length = self.size - pos

        self.offset += length
        self.read_bytes += length
        self.read_stats.append((pos, length))
        req = f'{self.data_req}&offset={pos}&length={length}'
        conn = http.client.HTTPConnection(self.data_url.netloc, blocksize=self.buffer_size)
        conn.request("GET", req)
        resp = conn.getresponse()
        data = resp.read(length)
        conn.close()
        return data

    def readinto(self, b):
        buffer = self.read(len(b))
        length = len(buffer)
        b[:length] = buffer
        return length

    def tell(self):
        return self.offset

    def seekable(self):
        return True

    def readable(self):
        return True

    def writable(self):
        return False

    def close(self):
        self.conn.close()

