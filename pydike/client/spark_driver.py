import pickle
import json
import struct
import pydike.client.spark_driver
import pydike.client.tpch


def ndp_reader(split_index, config_json):
    config = json.loads(config_json)
    print(config)
    return pydike.client.tpch.TpchSQL(config)


class DeSerializer:
    def load_stream(self, infile):
        length = infile.read(4)
        length = struct.unpack("!i", length)[0]
        s = infile.read(length)
        return s


class Serializer:
    def dump_stream(self, out_iter, outfile):
        out_iter.to_spark(outfile)


def create_spark_worker_command():
    func = ndp_reader
    deser = DeSerializer()
    ser = Serializer()

    command = (func, None, deser, ser)  # Format should be (func, profiler, deserializer, serializer)
    pickle_protocol = pickle.HIGHEST_PROTOCOL
    return pickle.dumps(command, pickle_protocol)


if __name__ == '__main__':
    command = pydike.client.spark_driver.create_spark_worker_command()
    print(command.hex())
