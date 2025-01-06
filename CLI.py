import os
import click
import json
import logging
import numpy as np
from database.services import _bulk_write
from asn.models import VisNode, VisCluster, VisASRank, VisPhyLink, VisLogicNode, VisLogicLink
from database.models import TableSelector
from config import Config
from logs import configure_log
from extension import mongo
from utils.geometry import calc_center_pos, calc_point_distance, cluster_by_distance
from collections import defaultdict


logger = logging.getLogger("cli")
STEP = 10


@click.group()
def endpoint():
    pass


@endpoint.group(name="nodes")
def nodes():
    pass


def group_nodes(node_path):
    asn_cty_map = defaultdict(list)
    nb_node = 0
    nb_unknown = 0
    pos_map = dict()
    with open(node_path, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue
            nb_node += 1
            asn, cty, lat, lon = line.strip().split(',')
            if cty == 'Unknown':
                nb_unknown += 1
                continue
            lat = float(lat)
            lon = float(lon)
            key = (asn, cty)
            asn_cty_map[key].append(nb_node)
            pos_map[nb_node] = np.array([lat, lon], dtype=np.double)
    logger.info(f"Total nodes: {nb_node}. Unknown: {nb_unknown}. Valid: {nb_node - nb_unknown}")
    node_dir, node_file = os.path.split(node_path)
    pfx, sfx = os.path.splitext(node_file)
    new_node_file = pfx + '-group' + sfx
    group_distance = 30 # km
    logger.info(f"Grouping nodes by distance {group_distance} km...")
    with open(os.path.join(node_dir, new_node_file), 'w') as tp:
        cluster_id = 0
        for key, idx_list in asn_cty_map.items():
            asn, cty = key
            cluster_list = cluster_by_distance(idx_list, pos_map, min_distance=group_distance)
            for cluster in cluster_list:
                cluster_id += 1
                position = calc_center_pos(cluster, pos_map)
                tp.write(f"{cluster_id},{asn},{cty},{",".join(map(lambda x: "%.4f" % x, position))}\n")
    logger.info(f"Total groups: {cluster_id}")


def form_cluster(node_path):
    pos_map = dict()
    with open(node_path, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue
            idx, asn, cty, lat, lon = line.strip().split(',')
            pos_map[idx] = np.array([float(lat), float(lon)], dtype=np.double)
    idx_list = list(pos_map.keys())
    cluster_distance = 50 # km
    logger.info(f"Clustering nodes by distance {cluster_distance} km.")
    cluster_list = cluster_by_distance(idx_list, pos_map, min_distance=cluster_distance)
    cluster_id = 0
    nidx2cidx = dict()
    node_dir = os.path.dirname(node_path)
    cluster_file = "as_CC_cluster.csv"
    with open(os.path.join(node_dir, cluster_file), 'w') as tp:
        for cluster in cluster_list:
            cluster_id += 1
            position = calc_center_pos(cluster, pos_map)
            for idx in cluster:
                nidx2cidx[idx] = cluster_id
            size = len(cluster)
            tp.write(f"{cluster_id},{size},{','.join(map(lambda x: "%.4f" % x, position))}\n")
    logger.info(f"Total clusters: {cluster_id}")
    # append cluster id to node file
    new_node_file = os.path.basename(node_path).replace('.csv', '-cluster.csv')
    with open(os.path.join(node_dir, new_node_file), 'w') as tp:
        with open(node_path, 'r') as fp:
            for line in fp:
                if line.startswith('#'):
                    continue
                idx, asn, cty, lat, lon = line.strip().split(',')
                cluster_id = nidx2cidx[idx]
                tp.write(f"{idx},{asn},{cty},{lat},{lon},{cluster_id}\n")


@nodes.command('import')
@click.option('--file-path', '-p', type=click.Path(exists=True), required=True)
def load_nodes(file_path):
    group_nodes(file_path)
    group_file_path = file_path.replace('.csv', '-group.csv')
    form_cluster(group_file_path)
    cluster_file_path = group_file_path.replace('.csv', '-cluster.csv')
    _nodes_table = TableSelector.get_nodes_table(name='default_sync')
    _nodes_table.delete_many({})
    op_list = list()
    nb_node = 0
    nb_inseted = 0
    with open(cluster_file_path, 'r') as fp:
        for line in fp:
            line = line.strip()
            if line.startswith('#'):
                continue
            nb_node += 1
            item = VisNode.from_line(line)
            op_list.append(item)
            if len(op_list) > STEP:
                res = _bulk_write(_nodes_table, op_list)
                if res > 0:
                    nb_inseted += res
                op_list.clear()
    if op_list:
        res = _bulk_write(_nodes_table, op_list)
        if res > 0:
            nb_inseted += res
        op_list.clear()
    logger.info(f"{os.path.basename(cluster_file_path)} has {nb_node} nodes, {nb_inseted} are imported.")


@endpoint.group(name="clusters")
def clusters():
    pass


@clusters.command('import')
@click.option('--file-path', '-p', type=click.Path(exists=True), required=True)
def load_clusters(file_path):
    _table = TableSelector.get_clusters_table(name='default_sync')
    _table.delete_many({})
    op_list = list()
    nb_cluster = 0
    nb_inserted = 0
    with open(file_path, 'r') as fp:
        for line in fp:
            line = line.strip()
            if line.startswith('#'):
                continue
            nb_cluster += 1
            item = VisCluster.from_line(line)
            op_list.append(item)
            if len(op_list) > STEP:
                res = _bulk_write(_table, op_list)
                if res > 0:
                    nb_inserted += res
                op_list.clear()
    if op_list:
        res = _bulk_write(_table, op_list)
        if res > 0:
            nb_inserted += res
        op_list.clear()
    logger.info(f"{os.path.basename(file_path)} has {nb_cluster} clusters, {nb_inserted} are imported.")


@endpoint.group(name="asrank")
def asrank():
    pass

@asrank.command('import')
@click.option('--file-path', '-p', type=click.Path(exists=True), required=True)
def load_asrank(file_path):
    logger.info(f"Importing ASRank data from {os.path.basename(file_path)}")
    op_list = list()
    _table = TableSelector.get_asrank_table(name='default_sync')
    _table.delete_many({})
    nb_record = 0
    nb_inserted = 0
    with open(file_path, 'r') as fp:
        for line in fp:
            line = line.strip()
            if line.startswith('#'):
                continue
            nb_record += 1
            item = VisASRank.from_line(line, idx=nb_record)
            op_list.append(item)
            if len(op_list) > STEP:
                res = _bulk_write(_table, op_list)
                if res > 0:
                    nb_inserted += res
                op_list.clear()
    if op_list:
        res = _bulk_write(_table, op_list)
        if res > 0:
            nb_inserted += res
        op_list.clear()
    logger.info(f"{os.path.basename(file_path)} has {nb_record} records, {nb_inserted} are imported.")


@endpoint.group(name="phy-links")
def phy_links():
    pass


def preprocess_phy_link(node_path, link_path):
    logger.info(f"Importing nodes from {os.path.basename(node_path)}")
    asn_cty_map = defaultdict(list)
    asn_cty_idxs = defaultdict(list)
    nidx2cidx = dict()
    with open(node_path, 'r') as fp:
        for line in fp:
            if line.startswith('#'):
                continue
            line = line.strip()
            item = VisNode.from_line(line)
            key = (item['asn'], item['country'])
            asn_cty_map[key].append(np.array([item['latitude'], item['longitude']], dtype=np.double))
            asn_cty_idxs[key].append(item['index'])
            nidx2cidx[item['index']] = item['cluster_id']
    logger.info(f"Importing phy-links from {os.path.basename(link_path)}")
    dup_link_map = dict()
    nb_link = 0
    nb_inserted = 0
    new_link_path = link_path.replace('.csv', '-group-cluster.csv')
    with open(new_link_path, 'w') as tp:
        with open(link_path, 'r') as fp:
            for line in fp:
                if line.startswith('#'):
                    continue
                nb_link += 1
                asn1, asn2, cty1, cty2, ltype = line.strip().split(',')
                asn1, asn2 = int(asn1), int(asn2)
                lkey = (asn1, cty1, asn2, cty2)
                if dup_link_map.get(lkey, None):
                    continue
                dup_link_map[lkey] = True
                key1 = (asn1, cty1)
                key2 = (asn2, cty2)
                idx_list1 = asn_cty_idxs.get(key1, None)
                idx_list2 = asn_cty_idxs.get(key2, None)
                pos_list1 = asn_cty_map.get(key1, None)
                pos_list2 = asn_cty_map.get(key2, None)
                min_dist = 1e6
                if pos_list1 is not None and pos_list2 is not None:
                    nb_inserted += 1
                    for i in range(len(pos_list1)):
                        for j in range(len(pos_list2)):
                            dist = calc_point_distance(pos_list1[i], pos_list2[j])
                            if dist < min_dist:
                                min_dist = dist
                                src_nidx = idx_list1[i]
                                dst_nidx = idx_list2[j]
                                src_ctr = pos_list1[i]
                                dst_ctr = pos_list2[j]
                                src_cidx = nidx2cidx[src_nidx]
                                dst_cidx = nidx2cidx[dst_nidx]
                    tp.write(f"{nb_inserted},{asn1},{asn2},{cty1},{cty2},{src_nidx},{dst_nidx},{src_cidx},{dst_cidx},{','.join(map(lambda x: "%.4f" % x, src_ctr))},{','.join(map(lambda x: "%.4f" % x, dst_ctr))},{ltype},{0 if src_cidx==dst_cidx else 1},{"%.4f" % min_dist}\n")
    logger.info(f"{os.path.basename(link_path)} has {nb_link} links, {nb_inserted} are imported.")


@phy_links.command('import')
@click.option('--node-path', '-n', type=click.Path(exists=True), required=True)
@click.option('--link-path', '-l', type=click.Path(exists=True), required=True)
def load_phy_links(node_path, link_path):
    # preprocess_phy_link(node_path, link_path)
    new_link_path = link_path.replace('.csv', '-group-cluster.csv')
    with open(new_link_path, 'r') as fp:
        _table = TableSelector.get_phy_links_table(name='default_sync')
        _table.delete_many({})
        op_list = list()
        nb_phy_link = 0
        for line in fp:
            line = line.strip()
            if line.startswith('#'):
                continue
            nb_phy_link += 1
            item = VisPhyLink.from_line(line)
            op_list.append(item)
            if len(op_list) > STEP:
                res = _bulk_write(_table, op_list)
                op_list.clear()
        if op_list:
            res = _bulk_write(_table, op_list)
            op_list.clear()


@endpoint.group(name="logic")
def logic():
    pass


@logic.command('import')
@click.option('--rel-path', '-r', type=click.Path(exists=True), required=True)
@click.option('--asrank-path', '-a', type=click.Path(exists=True), required=True)
def load_logic_links(asrank_path, rel_path):
    nb_logic_node = 0
    nb_logic_link = 0
    nb_node_inserted = 0
    nb_link_inserted = 0
    op_node_list = list()
    op_link_list = list()
    asn2idx = dict()
    link_type_mapped = {0: 'p2p', -1: 'p2c'}
    asrank_data = dict()

    _node_table = TableSelector.get_logic_nodes_table(name='default_sync')
    _link_table = TableSelector.get_logic_links_table(name='default_sync')
    _node_table.delete_many({})
    _link_table.delete_many({})

    with open(asrank_path, 'r') as fp:
        for line in fp:
            obj = json.loads(line.strip())
            obj['asn'] = int(obj['asn'])
            asrank_data[obj['asn']] = obj

    with open(rel_path, 'r') as fp:
        for line in fp:
            asn1, asn2, rel = map(int, line.strip().split(','))
            link_type = link_type_mapped[rel]
            src_asrank = asrank_data.get(asn1)
            if not asn2idx.get(asn1, False):
                nb_logic_node += 1
                op_node_list.append(VisLogicNode.to_obj(nb_logic_node, asn1, src_asrank))
                asn2idx[asn1] = nb_logic_node
            dst_asrank = asrank_data.get(asn2)
            if not asn2idx.get(asn2, False):
                nb_logic_node += 1
                op_node_list.append(VisLogicNode.to_obj(nb_logic_node, asn2, dst_asrank))
                asn2idx[asn2] = nb_logic_node
            nb_logic_link += 1
            op_link_list.append(VisLogicLink.to_obj(nb_logic_link, asn2idx[asn1], asn2idx[asn2], link_type, src_asrank, dst_asrank))
            if len(op_node_list) > STEP:
                res = _bulk_write(_node_table, op_node_list)
                if res > 0:
                    nb_node_inserted += res
                op_node_list.clear()
            if len(op_link_list) > STEP:
                res = _bulk_write(_link_table, op_link_list)
                if res > 0:
                    nb_link_inserted += res
                op_link_list.clear()
    if len(op_node_list) > 0:
        res = _bulk_write(_node_table, op_node_list)
        if res > 0:
            nb_node_inserted += res
        op_node_list.clear()
    if len(op_link_list) > 0:
        res = _bulk_write(_link_table, op_link_list)
        if res > 0:
            nb_link_inserted += res
        op_link_list.clear()
    logger.info(f"{os.path.basename(rel_path)} has {nb_logic_node} nodes, {nb_node_inserted} are imported.")
    logger.info(f"{os.path.basename(rel_path)} has {nb_logic_link} links, {nb_link_inserted} are imported.")


def configure():
    conf = Config.model_dump()
    logger.debug(f"Config mode={Config.MODE}")
    mongo.load_config(conf['MONGO_MAP'])
    configure_log(Config)


if __name__ == '__main__':
    configure()
    endpoint()
