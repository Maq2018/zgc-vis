
import csv
import argparse
import numpy as np
from io import StringIO
from utils.geometry import calc_point_distance


class Node:
    def __init__(self):
        self.name = ""
        self.organization = ""
        self.latitude = 0
        self.longitude = 0
        self.city = ""
        self.state = ""
        self.country = ""
        self.source = ""
        self.date = None

    def to_string(self):
        return f"\"{self.organization}\",\"{self.name}\",{self.latitude},{self.longitude},{self.city},{self.state},{self.country},{self.source},{self.date}"

class Cluster:
    def __init__(self):
        self.position = None
        self.count = 0
        self.indices = []

    def add(self, idx, pos):
        if self.position is None:
            self.position = pos.copy()
        self.count += 1
        self.indices.append(idx)


clusters = list()
nodes = list()


def form_cluster(ifile, ofile):
    keep_digits = 6
    min_distance = 10 # km

    def split_string(s):
        fp = StringIO(s)
        reader = csv.reader(fp, delimiter=',', quotechar='"')
        return next(reader)

    with open(ifile, "r") as f:
        for line in f:
            line = line.strip()
            items = split_string(line)
            try:
                assert len(items) == 9
                node = Node()
                node.name = items[1].strip("\"")
                node.organization = items[0].strip("\"")
                node.latitude = round(float(items[2]), keep_digits)
                node.longitude = round(float(items[3]), keep_digits)
                node.city = items[4]
                node.state = items[5]
                node.country = items[6]
                node.source = items[7]
                node.date = items[8]
                nodes.append(node)
            except Exception as e:
                pass

    print("Total nodes: ", len(nodes))
    positions = list(map(lambda x: np.array([x.latitude, x.longitude], dtype=np.double), nodes))
    for idx, pos in enumerate(positions):
        found = False
        for cluster in clusters:
            if calc_point_distance(cluster.position, pos) < min_distance:
                cluster.indices.append(idx)
                cluster.count += 1
                found = True
                break
        if not found:
            cluster = Cluster()
            cluster.add(idx, pos)
            clusters.append(cluster)
    
    cluster_size = list(map(lambda x: x.count, clusters))
    print("Total clusters: ", len(clusters))
    print("Max cluster size: ", max(cluster_size))
    print("Min cluster size: ", min(cluster_size))
    print("Avg cluster size: ", np.mean(cluster_size))
    print("Median cluster size: ", np.median(cluster_size))
    for pcnt in range(0, 105, 5):
        print(f"Percentile {pcnt}%: {np.percentile(cluster_size, pcnt)}")
    with open(ofile, 'w') as fp:
        for cluster in sorted(clusters, key=lambda x: x.count, reverse=True):
            indices = cluster.indices
            for idx in indices:
                fp.write(f"{nodes[idx].to_string()}\n")
            fp.write('#'*50 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", type=str, help="Input file path")
    parser.add_argument("-o", "--output", type=str, help="Output file path")
    args = parser.parse_args()
    form_cluster(args.file, args.output)
