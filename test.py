import math
import pandas as pd
import numpy as np
from pyspark import SparkContext
from operator import add

sc = SparkContext(appName="dsci551")

#Define Spark RDD
class RDD:
    def __init__(self, data, num_partitions):
        self.num_partitions = num_partitions
        self.partitions = self._partition_data(data)
        self.partition_locations = self._get_partition_locations(num_partitions)
        self.header = None
    
    def _partition_data(self, data):
        partition_size = len(data) // self.num_partitions
        partitions = []
        for i in range(self.num_partitions):
            start_index = i * partition_size
            end_index = (i + 1) * partition_size if i < self.num_partitions - 1 else len(data)
            partitions.append(data[start_index:end_index])
        return partitions
    
    def _get_partition_locations(self, num_partitions):
        partition_locations = {}
        for i in range(self.num_partitions):
            partition_locations[i] = "worker{}".format(i % num_partitions) # assume there are 3 worker nodes
        return partition_locations
