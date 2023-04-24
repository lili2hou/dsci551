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
    
    def __iter__(self):
        return iter([item for partition in self.partitions for item in partition])
    
    def map(self, func):
        new_partitions = []
        for partition in self.partitions:
            new_partition = []
            for item in partition:
                new_item = func(item)
                new_partition.append(new_item)
            new_partitions.append(new_partition)
        return RDD([item for partition in new_partitions for item in partition], self.num_partitions)
    
    def filter(self, func):
        new_partitions = []
        for partition in self.partitions:
            new_partition = []
            for item in partition:
                if func(item):
                    new_partition.append(item)
            new_partitions.append(new_partition)
        return RDD([item for partition in new_partitions for item in partition], self.num_partitions)
    
    def reduce(self, func):
        result = self.partitions[0][0]
        for partition in self.partitions:
            if partition == self.partitions[0]:
                for item in partition[1:]:
                    result = func(result, item)
            else:
                for item in partition:
                    result = func(result, item)
        return result
    
    def coalesce(self, new_partitions):
        new_data = []
        new_partitions = []
        new_locations = []
        for i in range(0, len(self.data), new_partitions):
            partition_data = self.data[i:i+new_partitions]
            partition_locations = self.locations[i:i+new_partitions]
            new_data.append(partition_data)
            new_partitions.append(len(new_data) - 1)
            new_locations.append(partition_locations[0] if all(l == partition_locations[0] for l in partition_locations) else None)
        return RDD(new_data, partitions=new_partitions, locations=new_locations)
    
    def min(self):
        min_value = self.partitions[0][0]
        for partition in self.partitions:
            for item in partition:
                if item < min_value:
                    min_value = item
        return min_value
    
    def max(self):
        max_value = self.partitions[0][0]
        for partition in self.partitions:
            for item in partition:
                if item > max_value:
                    max_value = item
        return max_value
    
    def sort(self, reverse=False):
        sorted_items = sorted([item for partition in self.partitions for item in partition], reverse=reverse)
        return RDD(sorted_items, self.num_partitions)
    
    def groupby(self, func):
        groups = {}
        for partition in self.partitions:
            for item in partition:
                key = func(item)
                if key not in groups:
                    groups[key] = []
                groups[key].append(item)
        return groups
    
    def average(self):
        total_sum = 0
        total_count = 0
        for partition in self.partitions:
            for item in partition:
                total_sum += item
                total_count += 1
        return total_sum / total_count
    
    def sum(self):
        total_sum = 0
        for partition in self.partitions:
            for item in partition:
                total_sum += item
        return total_sum
    
    def collect(self):
        result = []
        for partition in self.partitions:
            for item in partition:
                result.append(item)
        print(result)
        return result
    
    def count(self):
        count = 0
        for partition in self.partitions:
            count += len(partition)
        return count
    
    def take(self, n):
        taken = []
        for partition in self.partitions:
            for item in partition:
                taken.append(item)
                if len(taken) == n:
                    return taken
        return taken
    
    def aggregate(self, zero_value, seq_func, comb_func):
        acc = zero_value
        for partition in self.partitions:
            acc = seq_func(acc, partition)
        return comb_func(acc)

    
#Define Class SC
class SC:
    def __init__(self, app_name):
        self.app_name = app_name
        self.rdds = []

    def parallelize(self, data, filename, num_partitions=1, has_header=False):
        if data is not None:
            rdd = RDD(data, num_partitions)
            self.rdds.append(rdd)
            return rdd
        elif filename is not None:
            with open(filename, 'r', encoding='windows-1252') as f:
                lines = f.readlines()
            if has_header:
                lines = lines[1:]
            lines = [line.strip() for line in lines]
            return self.parallelize(data=lines, filename = filename, num_partitions=num_partitions)
        else:
            rdd = RDD([], num_partitions)
            self.rdds.append(rdd)
            return rdd
        
    def get_rdd(self, index):
        return self.rdds[index]  


lst = [{
    'name': ['Alice', 'Bob', 'Charlie', 'Andy', 'Kevin', 'James', 'Wade'],
    'age': [25, 30, 35, 18, 18, 29, 31],
    'gender': ['F', 'M', 'M', 'M', 'M', 'M', 'F'],
    'BankName': ['BOA', 'Chase', 'TB', 'BOA', 'BOA', 'Chase', 'Chase'],
    'Location': ['San Francisco', 'Boston', 'Dallas', 'Syracuse', 'Los Angeles', 'Cleveland', 'Miami']
}]

lst2 = [1,2,3,4,5,6] 
myrdd = SC("dsci551")

rdd = myrdd.parallelize(data = lst2, filename = None, num_partitions = 5)
rdd.collect()

def wonderful(x):
    return x + 2

pl = rdd._get_partition_locations(5)
print(pl)

def add(a, b): 
    return a + b
result = iter(rdd)
print(next(result))