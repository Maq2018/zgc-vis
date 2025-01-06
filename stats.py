
import os
import json
import random
import numpy as np
from collections import defaultdict
from utils.geometry import calc_point_distance, calc_center_pos, cluster_by_distance


KEEP_DIGITS = 4
ASRANK_PATH = "/home/maq18/Projects/Data/asrank/asns_20241215.jsonl"
NODE_PATH = "/home/maq18/Projects/Data/ServLoc-Trans/as_CC_location.csv"
LINK_PATH = "/home/maq18/Projects/Data/ServLoc/as_CC_facility.csv"
LOGIC_LINK_PATH = "/home/maq18/Projects/Data/ServLoc/as_relationship.csv"
LINK_TYPES = ["Direct",  "IXP", "submarine-cable"]
CLUSTER_DISTANCE = 30 # km


def toFloat(li):
    if type(li) is not list:
        return round(float(li), KEEP_DIGITS)
    return list(map(lambda x: round(float(x), KEEP_DIGITS), li))


def count_dup_links():
    nb_links = 0
    link_map = defaultdict(int)
    with open(LINK_PATH, 'r') as fp:
        for line in fp:
            asn1, asn2, cty1, cty2, ltype = line.strip().split(',')
            key = (asn1, cty1, asn2, cty2)
            link_map[key] += 1
            nb_links += 1
    print(f"Total links: {nb_links}")
    print(f"Unique links: {len(link_map)}")


def calc_phy_link_dist():
    debug = True
    asn_cty_map = defaultdict(list)
    nb_node = 0
    nb_unknown = 0
    nb_invalid = 0
    with open(NODE_PATH, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue
            nb_node += 1
            asn, cty, lat, lon = line.strip().split(',')
            if (not asn.isdigit()):
                nb_invalid += 1
                continue
            if cty == 'Unknown':
                nb_unknown += 1
                continue
            lat, lon = toFloat([lat, lon])
            key = (asn, cty)
            asn_cty_map[key].append(np.array([lat, lon], dtype=np.double))
    print(f"Total nodes: {nb_node}. Unknown: {nb_unknown}. Invalid: {nb_invalid}. Valid: {nb_node - nb_unknown - nb_invalid}")
    nb_phy_link = 0
    nb_inserted = 0
    link_dir, link_file = os.path.split(LINK_PATH)
    new_link_file = link_file.replace(".csv", "_dist.csv")
    with open(LINK_PATH, 'r') as fp:
        with open(os.path.join(link_dir, new_link_file), 'w') as tp:
            for line in fp:
                if line.startswith('#'):
                    continue
                nb_phy_link += 1
                if debug and (nb_phy_link % 10000==0):
                    print(f"Processed {nb_phy_link} links.")
                asn1, asn2, cty1, cty2, ltype = line.strip().split(',')
                pos_list1 = asn_cty_map.get((asn1, cty1), None)
                pos_list2 = asn_cty_map.get((asn2, cty2), None)
                min_dist = 1e6
                if pos_list1 is not None and pos_list2 is not None:
                    nb_inserted += 1
                    for i in range(len(pos_list1)):
                        for j in range(len(pos_list2)):
                            dist = calc_point_distance(pos_list1[i], pos_list2[j])
                            if dist < min_dist:
                                min_dist = dist
                                center1 = pos_list1[i]
                                center2 = pos_list2[j]
                    tp.write(f"{asn1}\t{asn2}\t{cty1}\t{cty2}\t{ltype}\t{",".join(map(str, center1))}\t{",".join(map(str, center2))}\t{round(dist, KEEP_DIGITS)}\n")


def count_distance(sz: int):
    select_ltype = LINK_TYPES[sz]
    link_dist_path = LINK_PATH.replace(".csv", "-group-cluster.csv")
    with open(link_dist_path, 'r') as fp:
        dist_list = []
        for line in fp:
            item_list = line.strip().split(',')
            ltype = item_list[-3]
            distance = round(float(item_list[-1]), 4)
            # if ltype == select_ltype:
            dist_list.append(distance)
    print('#'*20 + f" {select_ltype} links " + '#'*20)
    print(f"Total direct links: {len(dist_list)}")
    print(f"Average distance: {np.mean(dist_list)}")
    print(f"Max distance: {np.max(dist_list)}")
    print(f"Min distance: {np.min(dist_list)}")
    print(f"Median deviation: {np.median(dist_list)}")
    print(f"Standard deviation: {np.std(dist_list)}")
    for pcnt in range(0, 105,5):
        print(f"{pcnt}%\t{np.round(np.percentile(dist_list, pcnt), 2)}\t")


def group_node():
    asn_cty_map = defaultdict(list)
    nb_node = 0
    nb_unknown = 0
    pos_map = dict()
    with open(NODE_PATH, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue
            nb_node += 1
            asn, cty, lat, lon = line.strip().split(',')
            if cty == 'Unknown':
                nb_unknown += 1
                continue
            lat, lon = toFloat([lat, lon])
            key = (asn, cty)
            asn_cty_map[key].append(nb_node)
            pos_map[nb_node] = np.array([lat, lon], dtype=np.double)
    print(f"Total nodes: {nb_node}. Unknown: {nb_unknown}. Valid: {nb_node - nb_unknown}")
    node_dir, node_file = os.path.split(NODE_PATH)
    new_node_dir = node_dir + '-Trans'
    with open(os.path.join(new_node_dir, node_file), 'w') as tp:
        cluster_id = 0
        for key, idx_list in asn_cty_map.items():
            asn, cty = key
            cluster_list = cluster_by_distance(idx_list, pos_map)
            for cluster in cluster_list:
                cluster_id += 1
                position = calc_center_pos(cluster, pos_map)
                tp.write(f"{asn},{cty},{",".join(map(str, position))}\n")
    print(f"Total clusters: {cluster_id}")


if __name__ == "__main__":
    # for i in range(3):
    count_distance(0)
