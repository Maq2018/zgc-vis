import os
import click
import json
import logging
import numpy as np
from database.services import _bulk_write
from asn.models import (
    VisPhysicalNode, 
    VisSubmarineCable, 
    VisLandingPoint, 
    VisLandCable, 
    VisLogicNode, 
    VisLogicLink, 
    VisPop, 
    VisPhysicalLink,
    VisCity
)
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
        next(fp)
        for line in fp:
            phyical_node_obj = VisPhysicalNode.from_line(line)
            if phyical_node_obj:
                phyical_node_obj['index'] = nb_line
                nb_line += 1
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
        next(fp)
        for line in fp:
            cable_obj = VisLandCable.from_line(line)
            if cable_obj is not None:
                VisLandCable.fill_unknown_fields(cable_obj, nb_line)
                nb_line += 1
            op_list.append(cable_obj)
            if len(op_list) > STEP:
                nb_cable += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_cable += _bulk_write(_table, op_list)
        op_list.clear()

    logger.info(f"{os.path.basename(file)} has {nb_line} records, {nb_cable} are imported.")


@endpoint.group(name="pop")
def pop():
    pass


@pop.command('import')
@click.option('--file', '-f', type=click.Path(exists=True), required=True)
def load_pop(file):
    _table = TableSelector.get_pop_table(name='default_sync')
    _table.delete_many({})

    nb_pop = 0
    op_list = list()

    with open(file, 'r') as fp:
        next(fp)
        for line in fp:
            pop_obj = VisPop.from_line(line)
            op_list.append(pop_obj)
            if len(op_list) > STEP:
                nb_pop += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_pop += _bulk_write(_table, op_list)
        op_list.clear()

    logger.info(f"{os.path.basename(file)} has {nb_pop} records.")


@endpoint.group(name="phy-conn")
def phy_conn():
    pass


@phy_conn.command('import')
@click.option('--file', '-f', type=click.Path(exists=True), required=True)
def load_phy_conn(file):
    _table = TableSelector.get_phy_links_table(name='default_sync')
    _table.delete_many({})

    nb_link = 0
    op_list = list()

    with open(file, 'r') as fp:
        next(fp)
        for line in fp:
            link_obj = VisPhysicalLink.from_line(line)
            op_list.append(link_obj)
            if len(op_list) > STEP:
                nb_link += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_link += _bulk_write(_table, op_list)
        op_list.clear()

    logger.info(f"{os.path.basename(file)} has {nb_link} records.")


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
                nb_node_inserted += _bulk_write(_node_table, op_node_list)
                op_node_list.clear()
            if len(op_link_list) > STEP:
                nb_link_inserted += _bulk_write(_link_table, op_link_list)
                op_link_list.clear()
    if len(op_node_list) > 0:
        nb_node_inserted += _bulk_write(_node_table, op_node_list)
        op_node_list.clear()
    if len(op_link_list) > 0:
        nb_link_inserted += _bulk_write(_link_table, op_link_list)
        op_link_list.clear()
    logger.info(f"{os.path.basename(rel_path)} has {nb_logic_node} nodes, {nb_node_inserted} are imported.")
    logger.info(f"{os.path.basename(rel_path)} has {nb_logic_link} links, {nb_link_inserted} are imported.")


@endpoint.group(name="city")
def city():
    pass


@city.command('import')
@click.option('--file', '-f', type=click.Path(exists=True), required=True)
def load_city(file):
    _table = TableSelector.get_city_table(name='default_sync')
    _table.delete_many({})

    idx = 0
    nb_city = 0
    op_list = list()

    with open(file, 'r') as fp:
        next(fp)
        for line in fp:
            city_obj = VisCity.from_line(line)
            if city_obj is not None:
                VisCity.fill_unknown_fields(city_obj, idx)
                idx += 1
            op_list.append(city_obj)
            if len(op_list) > STEP:
                nb_city += _bulk_write(_table, op_list)
                op_list.clear()
    if len(op_list) > 0:
        nb_city += _bulk_write(_table, op_list)
        op_list.clear()

    logger.info(f"{os.path.basename(file)} has {nb_city} records.")


def configure():
    conf = Config.model_dump()
    logger.debug(f"Config mode={Config.MODE}")
    mongo.load_config(conf['MONGO_MAP'])
    configure_log(Config)


if __name__ == '__main__':
    configure()
    endpoint()
