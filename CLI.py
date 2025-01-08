import os
import click
import json
import logging
import numpy as np
from database.services import _bulk_write
from asn.models import VisPhysicalNode, VisSubmarineCable, VisLandingPoint, VisLandCable, VisLogicNode, VisLogicLink
from database.models import TableSelector
from config import Config
from logs import configure_log
from extension import mongo
from utils.geometry import calc_center_pos, calc_point_distance, cluster_by_distance
from collections import defaultdict


logger = logging.getLogger("cli")
STEP = 30
SKIP = 1


@click.group()
def endpoint():
    pass


@endpoint.group(name="physical-nodes")
def physical_nodes():
    pass


@physical_nodes.command('import')
@click.option('--file', '-f', type=click.Path(exists=True), required=True)
def import_physical_nodes(file):
    _table = TableSelector.get_physical_nodes_table(name='default_sync')
    _table.delete_many({})

    nb_line = 0
    nb_node = 0
    op_list = list()
    with open(file, 'r') as fp:
        for line in fp:
            nb_line += 1
            if nb_line <= SKIP:
                continue
            phyical_node_obj = VisPhysicalNode.from_line(line)
            if phyical_node_obj:
                phyical_node_obj['index'] = nb_line
            op_list.append(phyical_node_obj)
            if len(op_list) > STEP:
                nb_node += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_node += _bulk_write(_table, op_list)
        op_list.clear()
    
    logger.info(f"{os.path.basename(file)} has {nb_line} records, {nb_node} are imported.")


@endpoint.group(name="submarine-cables")
def submarine_cables():
    pass


@submarine_cables.command('import')
@click.option('--file', '-f', type=click.Path(exists=True), required=True)
def import_submarine_cable(file):
    _table = TableSelector.get_submarine_cables_table(name='default_sync')
    _table.delete_many({})

    nb_line = 0
    nb_cable = 0
    op_list = list()

    with open(file, 'r') as fp:
        for line in fp:
            nb_line += 1
            if nb_line <= SKIP:
                continue
            cable_obj = VisSubmarineCable.from_line(line)
            op_list.append(cable_obj)
            if len(op_list) > STEP:
                nb_cable += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_cable += _bulk_write(_table, op_list)
        op_list.clear()

    logger.info(f"{os.path.basename(file)} has {nb_line} records, {nb_cable} are imported.")


@endpoint.group(name="landing-points")
def landing_points():
    pass


@landing_points.command('import')
@click.option('--file', '-f', type=click.Path(exists=True), required=True)
def import_landing_points(file):
    _table = TableSelector.get_landing_points_table(name='default_sync')
    _table.delete_many({})

    nb_line = 0
    nb_point = 0
    op_list = list()

    with open(file, 'r') as fp:
        for line in fp:
            nb_line += 1
            if nb_line <= SKIP:
                continue
            point_obj = VisLandingPoint.from_line(line)
            # Todo: when loading file with complete information, remove fill_unknown_fields
            if point_obj is not None:
                VisLandingPoint.fill_unknown_fields(point_obj, nb_line)
            op_list.append(point_obj)
            if len(op_list) > STEP:
                nb_point += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_point += _bulk_write(_table, op_list)
        op_list.clear()

    logger.info(f"{os.path.basename(file)} has {nb_line} records, {nb_point} are imported.")


@endpoint.group(name="land-cables")
def land_cables():
    pass


@land_cables.command('import')
@click.option('--file', '-f', type=click.Path(exists=True), required=True)
def import_land_cables(file):
    _table = TableSelector.get_land_cables_table(name='default_sync')
    _table.delete_many({})

    nb_line = 0
    nb_cable = 0
    op_list = list()

    with open(file, 'r') as fp:
        for line in fp:
            nb_line += 1
            if nb_line <= SKIP:
                continue
            cable_obj = VisLandCable.from_line(line)
            if cable_obj is not None:
                VisLandCable.fill_unknown_fields(cable_obj, nb_line)
            op_list.append(cable_obj)
            if len(op_list) > STEP:
                nb_cable += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_cable += _bulk_write(_table, op_list)
        op_list.clear()

    logger.info(f"{os.path.basename(file)} has {nb_line} records, {nb_cable} are imported.")


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
