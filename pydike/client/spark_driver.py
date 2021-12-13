import pickle
import json
import pydike.client.spark_driver
import pydike.client.tpch

from pyspark.serializers import write_with_length, write_int, read_long, read_bool, \
    write_long, read_int, SpecialLengths, UTF8Deserializer, PickleSerializer, \
    BatchedSerializer


def ndp_reader(split_index, config_json):
    config = json.loads(config_json)
    print(config)
    return dike.client.tpch.TpchSQL(config)


utf8_deserializer = UTF8Deserializer()


class DeSerializer:
    def load_stream(self, infile):
        return utf8_deserializer.loads(infile)


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