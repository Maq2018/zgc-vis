
import os
import csv
import json
import rtree
import random
import argparse
import subprocess
import numpy as np
import networkx as nx
from io import StringIO
from datetime import datetime
from collections import defaultdict
from scipy.spatial import KDTree
from utils.geometry import cluster_by_distance, calc_point_distance, cluster_by_distance_dbscan
from utils.conversion import literal_eval, to_wkt_multilinestring
from shapely.geometry import Point, Polygon
from shapely.wkt import loads


CONTINENTS = ['AF', 'AN', 'AS', 'EU', 'NA', 'OC', 'SA', 'IC', 'OS']
SUBMARINE_DISTANCE = 5000  # km
KEEP_DIGITS_DIS = 2
KEEP_DIGIT_DIM = 4
MAPPING_FAC_DISTANCE = 30  # km
MAPPING_CITY_DISTANCE = 80  # km
MAPPING_LANDING_PTS_DISTANCE = 20  # km


def extract_interdomain_links(node_as_fpath, node_geo_fpath, link_fpath):
    print('Extracting inter-domain links...')
    # load node to AS mapping
    base_dir, node_as_fname = os.path.split(node_as_fpath)
    _, node_geo_fname = os.path.split(node_geo_fpath)
    _, link_fname = os.path.split(link_fpath)
    tmp_dir = base_dir.replace('Base', 'Tmp')
    as_dict = dict()
    node2as = dict()
    node2geo = dict()
    with open(node_as_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            starter, nid, asn, tag = line.strip().split()
            assert starter == 'node.AS'
            nid = int(nid.strip('N'))
            asn = int(asn)
            node2as[nid] = asn
            as_dict[asn] = 1
    print('  {} has {} nodes, representing {} ASes.'.format(
        node_as_fname, len(node2as), len(as_dict)))
    # load node to geo mapping
    with open(node_geo_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            starter, nid = items[0].strip().split()
            assert starter == 'node.geo'
            nid = int(nid.strip('N:'))
            lat, lon = items[5:7]
            node2geo[nid] = (float(lat), float(lon))
    print('  {} has {} nodes with geolocation.'.format(
        node_geo_fname, len(node2geo)))
    # get nodes with both AS and geo info
    node_ids = set(node2as.keys()) & set(node2geo.keys())
    print('  {} nodes with both AS and geo info.'.format(len(node_ids)))
    # sub_node_ids = set()
    nb_useful_links = 0
    with open(link_fpath, 'r') as file1, open(os.path.join(tmp_dir, link_fname), 'w') as file2:
        for line in file1:
            if line.startswith('#'):
                continue
            items = line.strip().split()
            assert items[0] == 'link'
            member_list = list()
            for member in items[2:]:
                node_id = int(member.strip('N')) if ':' not in member else int(
                    member.split(':')[0].strip('N'))
                member_list.append(node_id)
            member_list = list(set(member_list))
            asn_list = [node2as[nid] for nid in member_list if nid in node_ids]
            asn_list = list(set(asn_list))
            if len(asn_list) > 1:
                nb_useful_links += 1
                valid_member_list = [
                    nid for nid in member_list if nid in node_ids]
                # sub_node_ids.update(valid_member_list)
                file2.write("link {} {}\n".format(items[1], ' '.join(
                    ['N{}'.format(nid) for nid in valid_member_list])))
    print('  Extracted {} inter-domain links'.format(nb_useful_links))
    # print('Extracted {} inter-domain nodes'.format(len(sub_node_ids)))
    print('  Extracted {} inter-domain nodes'.format(len(node_ids)))
    with open(os.path.join(tmp_dir, node_as_fname), 'w') as f:
        # for nid in sub_node_ids:
        for nid in node_ids:
            f.write('node.AS N{} {}\n'.format(nid, node2as[nid]))
    with open(node_geo_fpath, 'r') as file1, open(os.path.join(tmp_dir, node_geo_fname), 'w') as file2:
        for line in file1:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            nid = int(items[0].split()[-1].strip('N:'))
            # if nid in sub_node_ids:
            if nid in node_ids:
                file2.write(line)


def group_proximity_nodes(node_as_fpath, node_geo_fpath, link_fpath):
    print('Grouping proximity nodes...')
    tmp_dir, node_as_fname = os.path.split(node_as_fpath)
    _, node_geo_fname = os.path.split(node_geo_fpath)
    _, link_fname = os.path.split(link_fpath)
    target_dir = tmp_dir.replace('Tmp', 'Target')
    # load node to geo mapping
    node2geo = dict()
    with open(node_geo_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            nid = int(items[0].split()[-1].strip('N:'))
            lat, lon = items[5:7]
            node2geo[nid] = np.array([float(lat), float(lon)], dtype=np.double)
    # load node to AS mapping, group nodes by AS
    node2as = dict()
    nodes_per_as = defaultdict(list)
    with open(node_as_fpath, 'r') as f:
        for line in f:
            _, nid, asn = line.strip().split()
            nid = int(nid.strip('N'))
            node2as[nid] = int(asn)
            nodes_per_as[asn].append(nid)
    print('  Loaded {} nodes, {} ASes.'.format(
        len(node2as), len(nodes_per_as)))
    # group nodes within an AS by proximity
    step = 0
    cluster_mapping = dict()
    for asn, nid_list in nodes_per_as.items():
        clusters = cluster_by_distance(nid_list, node2geo, min_distance=20)
        for cluster in clusters:
            for nid in cluster:
                cluster_mapping[nid] = cluster[0]
        step += 1
        if step % 100 == 0:
            print('  Processed {} ASes.'.format(step))
    del nodes_per_as
    # write grouped nodes.as
    nb_grouped_nodes = 0
    with open(os.path.join(target_dir, node_as_fname), 'w') as f:
        for nid, asn in node2as.items():
            if cluster_mapping[nid] == nid:
                nb_grouped_nodes += 1
                f.write('node.AS N{} {}\n'.format(nid, asn))
    # write grouped nodes.geo
    nb_grouped_nodes = 0
    with open(node_geo_fpath, 'r') as file1, open(os.path.join(target_dir, node_geo_fname), 'w') as file2:
        for line in file1:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            nid = int(items[0].split()[-1].strip('N:'))
            if cluster_mapping[nid] == nid:
                nb_grouped_nodes += 1
                file2.write(line)
    print('  Grouped {} nodes'.format(nb_grouped_nodes))
    nb_valid_link = 0
    with open(link_fpath, 'r') as file1, open(os.path.join(target_dir, link_fname), 'w') as file2:
        for line in file1:
            if line.startswith('#'):
                continue
            items = line.strip().split()
            assert items[0] == 'link'
            member_list = [int(member.strip('N')) for member in items[2:]]
            id_mapping_list = [cluster_mapping[nid] for nid in member_list]
            id_mapping_list = list(set(id_mapping_list))
            if len(id_mapping_list) > 1:
                nb_valid_link += 1
                file2.write('link L{}: {}\n'.format(nb_valid_link, ' '.join(
                    ['N{}'.format(nid) for nid in id_mapping_list])))
    print('  Grouped {} links'.format(nb_valid_link))


def remove_redundant_links(link_fpath, node_as_fpath):
    print('Removing redundant links...')
    # load node to AS mapping
    node2as = dict()
    with open(node_as_fpath, 'r') as f:
        for line in f:
            _, nid, asn = line.strip().split()
            nid = int(nid.strip('N'))
            node2as[nid] = int(asn)
    # read link files, format links into tuple format, and remove duplicate links
    target_dir, link_file = os.path.split(link_fpath)
    unique_link_file = link_file.replace('links', 'unique_links')
    link_dict = dict()
    nb_unique_links = 0
    with open(link_fpath, 'r') as file1, open(os.path.join(target_dir, unique_link_file), 'w') as file2:
        for line in file1:
            items = line.strip().split()
            members = list(sorted(items[2:]))
            link_type = "IXP" if len(members) > 2 else "Others"
            for i in range(len(members)):
                for j in range(i+1, len(members)):
                    asn_i = node2as[int(members[i].strip('N'))]
                    asn_j = node2as[int(members[j].strip('N'))]
                    if asn_i == asn_j:
                        continue
                    key = (members[i], members[j])
                    if key not in link_dict:
                        nb_unique_links += 1
                        link_dict[key] = 0
                        file2.write("link L{}: {} {}\n".format(
                            nb_unique_links, ' '.join(key), link_type))
    print('  Extracted {} unique links'.format(nb_unique_links))


def complete_phynode_city_info(city_fpath, facility_fpath):
    print('Completing physical node city info...')
    city_geo = list()
    city_label = list()
    city_dict = dict()
    with open(city_fpath, 'r') as f:
        next(f)
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            items = next(reader)
            city, state, country, lat, lon = items
            city_geo.append([float(lat), float(lon)])
            city_label.append([city, state, country])
            city_dict[(city, state, country)] = 1
    city_geo = np.array(city_geo, dtype=np.double)
    tree = KDTree(city_geo)

    nb_consistent = 0
    nb_unconsistent = 0
    nb_fields_matched = [0, 0, 0, 0]
    complete_facility_fpath = facility_fpath.replace('.csv', '_complete.csv')
    with open(complete_facility_fpath, 'w') as ofp:
        ofp.write(
            'Organization,Node Name,Latitude,Longitude,City,State,Country,Source,As of Date\n')
        writer = csv.writer(ofp, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
        with open(facility_fpath, 'r') as ifp:
            next(ifp)
            for line in ifp:
                line = StringIO(line.strip())
                reader = csv.reader(line, delimiter=',', quotechar='"')
                items = next(reader)
                organization, node_name, latitude, longitude, city, state, country, source, asof_date = items
                try:
                    coord = np.array(
                        [float(latitude), float(longitude)], dtype=np.double)
                except:
                    continue
                _, idx = tree.query(coord)
                content = [organization, node_name, latitude, longitude]
                content.extend(city_label[idx])
                content.extend([source, asof_date])
                writer.writerow(content)
                if city_dict.get((city, state, country)) is None:
                    nb_unconsistent += 1
                else:
                    nb_consistent += 1
                matched = 0
                if city == city_label[idx][0]:
                    matched += 1
                if state == city_label[idx][1]:
                    matched += 1
                if country == city_label[idx][2]:
                    matched += 1
                nb_fields_matched[matched] += 1
    print('  Consistent: {}, Unconsistent: {}'.format(
        nb_consistent, nb_unconsistent))
    print('  Matched fields: {}'.format(nb_fields_matched))


def simplify_line_string(landcable_fpath, city_file):
    print("Simplifying line string...")
    base_dir, file_name = os.path.split(landcable_fpath)
    simplified_landcable_fpath = os.path.join(
        base_dir, 'simplified_{}'.format(file_name))
    nb_line = 0
    content = list()

    # city_geo = list()
    # city_label = list()
    city_dict = dict()
    with open(city_file, 'r') as f:
        next(f)
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            items = next(reader)
            city, state, country, lat, lon = items
            city_dict[(city, state, country)] = 1
    #         city_geo.append([float(lat), float(lon)])
    #         city_label.append([city, state, country])
    # city_geo = np.array(city_geo, dtype=np.double)
    # tree = KDTree(city_geo)

    nb_consistent = 0
    nb_unconsistent = 0
    with open(landcable_fpath, 'r') as file1, open(simplified_landcable_fpath, 'w') as file2:
        file2.write(
            'FROM_CITY,FROM_STATE,FROM_COUNTRY,TO_CITY,TO_STATE,TO_COUNTRY,DISTANCE_KM,PATH_WKT,ASOF_DATE\n')
        writer = csv.writer(file2, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        next(file1)
        for line in file1:
            line = line.strip()
            nb_line += 1
            line_string = StringIO(line)
            reader = csv.reader(line_string, delimiter=',', quotechar='"')
            from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt, asof_date = next(
                reader)
            linestring = loads(path_wkt)
            tolerance = 0.1
            simplified = linestring.simplify(
                tolerance, preserve_topology=False)
            if city_dict.get((from_city, from_state, from_country)) is None or city_dict.get((to_city, to_state, to_country)) is None:
                nb_unconsistent += 1
            else:
                nb_consistent += 1
            # coords = np.array(simplified.coords)
            # _from_city, _from_state, _from_country = city_label[tree.query(coords[0])[1]]
            # _to_city, _to_state, _to_country = city_label[tree.query(coords[-1])[1]]
            # if from_city == _from_city and to_city == _to_city and from_state == _from_state and to_state == _to_state and from_country == _from_country and to_country == _to_country:
            #     nb_consistent += 1
            # else:
            #     nb_unconsistent += 1
            #     from_city, from_state, from_country = _from_state, _from_state, _from_country
            #     to_city, to_state, to_country = _to_city, _to_state, _to_country
            content.append([from_city, from_state, from_country, to_city,
                           to_state, to_country, distance_km, simplified.wkt, asof_date])
            if len(content) >= 30:
                writer.writerows(content)
                content.clear()
        if len(content) > 0:
            writer.writerows(content)
            content.clear()
    print('  Consistent: {}, Unconsistent: {}'.format(
        nb_consistent, nb_unconsistent))


def map_pop_to_facility(node_geo_fpath, facility_path, node_facility_fpath):
    print('Mapping PoPs to facility...')
    # load node to geo mapping
    node2geo = dict()
    with open(node_geo_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            nid = int(items[0].split()[-1].strip('N:'))
            lat, lon = items[5:7]
            node2geo[nid] = np.array([float(lat), float(lon)], dtype=np.double)
    # load facility geo
    facility_geo = list()
    with open(facility_path, 'r') as f:
        next(f)
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            items = next(reader)
            try:
                facility_geo.append([float(items[2]), float(items[3])])
            except:
                continue
    # map node to facility
    mapping_res = defaultdict(list)
    facility_geo = np.array(facility_geo, dtype=np.double)
    tree = KDTree(facility_geo)
    for nid, geo in node2geo.items():
        # extract the nearest 3 facilities to eliminate bias caused by kdtree
        _, indices = tree.query(geo, k=3)
        for idx in indices:
            distance = calc_point_distance(geo, facility_geo[idx])
            mapping_res[nid].append((idx, distance))
    # write mapping result
    with open(node_facility_fpath, 'w') as f:
        for nid, mapping in sorted(mapping_res.items(), key=lambda x: x[0]):
            idx, distance = min(mapping, key=lambda x: x[1])
            f.write('node.Facility N{} F{} {}\n'.format(
                nid, idx, round(distance, KEEP_DIGITS_DIS)))


def map_pop_to_city(node_geo_fpath, city_fpath, node_city_fpath):
    print('Mapping PoPs to city...')
    # load node to geo mapping
    node2geo = dict()
    with open(node_geo_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            nid = int(items[0].split()[-1].strip('N:'))
            lat, lon = items[5:7]
            node2geo[nid] = np.array([float(lat), float(lon)], dtype=np.double)
    # load city geo
    city_geo = list()
    with open(city_fpath, 'r') as f:
        next(f)
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            items = next(reader)
            try:
                city_geo.append([float(items[3]), float(items[4])])
            except:
                continue
    # map node to city
    mapping_res = defaultdict(list)
    city_geo = np.array(city_geo, dtype=np.double)
    tree = KDTree(city_geo)
    for nid, geo in node2geo.items():
        _, indices = tree.query(geo, k=3)
        for idx in indices:
            distance = calc_point_distance(geo, city_geo[idx])
            mapping_res[nid].append((idx, distance))
    with open(node_city_fpath, 'w') as f:
        for nid, mapping in sorted(mapping_res.items(), key=lambda x: x[0]):
            idx, distance = min(mapping, key=lambda x: x[1])
            f.write('node.City N{} C{} {}\n'.format(
                nid, idx, round(distance, KEEP_DIGITS_DIS)))


def map_pop_to_landing_points(node_geo_fpath, landing_pts_fpath, node_landing_pts_fpath):
    print('Mapping PoPs to landing points...')
    # load node to geo mapping
    node2geo = dict()
    with open(node_geo_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            nid = int(items[0].split()[-1].strip('N:'))
            lat, lon = items[5:7]
            node2geo[nid] = np.array([float(lat), float(lon)], dtype=np.double)
    # load landing points geo
    landing_pts_geo = list()
    with open(landing_pts_fpath, 'r') as f:
        landing_pts_list = json.load(f)
        for lpts_item in landing_pts_list:
            lpts_geo = lpts_item['geometry']
            coordinates = lpts_geo.split('coordinates=')[1].split(']')[
                0].strip('[').split(',')
            coordinates = [float(x.strip()) for x in coordinates]
            coordinates.reverse()
            assert len(coordinates) == 2
            landing_pts_geo.append(coordinates)
    landing_pts_geo = np.array(landing_pts_geo, dtype=np.double)
    tree = KDTree(landing_pts_geo)
    # map node to landing points
    mapping_res = defaultdict(list)
    for nid, geo in node2geo.items():
        _, indices = tree.query(geo, k=3)
        for idx in indices:
            distance = calc_point_distance(geo, landing_pts_geo[idx])
            mapping_res[nid].append((idx, distance))

    with open(node_landing_pts_fpath, 'w') as f:
        for nid, mapping in sorted(mapping_res.items(), key=lambda x: x[0]):
            idx, distance = min(mapping, key=lambda x: x[1])
            f.write('node.landing_points N{} LP{} {}\n'.format(
                nid, idx, distance))


def analyze_facility_mapping_distance(node_facility_fpath):
    print("Analyzing facility mapping distance...")
    distance_array = list()
    with open(node_facility_fpath, 'r') as fp:
        for line in fp:
            items = line.strip().split()
            starter = items[0]
            assert starter == 'node.Facility'
            distance_array.append(float(items[-1]))
    np_dis_arr = np.array(distance_array, dtype=np.double)
    print("  " + "#" * 50)
    print('  Mean distance: {}'.format(np.mean(np_dis_arr)))
    print('  Median distance: {}'.format(np.median(np_dis_arr)))
    print('  Max distance: {}'.format(np.max(np_dis_arr)))
    print('  Min distance: {}'.format(np.min(np_dis_arr)))
    print('  Std distance: {}'.format(np.std(np_dis_arr)))
    for pcnt in range(0, 105, 5):
        print('  Percentile {}\t{}\t'.format(
            pcnt, np.percentile(np_dis_arr, pcnt, axis=0)))


def analyze_city_mapping_distance(node_city_fpath):
    print("Analyzing city mapping distance...")
    distance_array = list()
    with open(node_city_fpath, 'r') as fp:
        for line in fp:
            items = line.strip().split()
            starter = items[0]
            assert starter == 'node.City'
            distance_array.append(float(items[-1]))
    np_dis_arr = np.array(distance_array, dtype=np.double)
    print("  " + "#" * 50)
    print('  Mean distance: {}'.format(np.mean(np_dis_arr)))
    print('  Median distance: {}'.format(np.median(np_dis_arr)))
    print('  Max distance: {}'.format(np.max(np_dis_arr)))
    print('  Min distance: {}'.format(np.min(np_dis_arr)))
    print('  Std distance: {}'.format(np.std(np_dis_arr)))
    for pcnt in range(0, 105, 5):
        print('  Percentile {}\t{}\t'.format(
            pcnt, np.percentile(np_dis_arr, pcnt, axis=0)))


def map_link2cable(link_fpath, node_as_fpath, node_city_fpath, city_fpath, landcable_fpath, submarinecable_fpath, landing_pts_path, cable_geo_fpath, link_cable_fpath):
    print("Mapping link to cable...")
    # init multi-graph
    G = nx.MultiGraph()

    # load node as info
    node2as = dict()
    with open(node_as_fpath, 'r') as f:
        for line in f:
            starter, nid, asn = line.strip().split()
            assert starter == 'node.AS'
            node2as[int(nid.strip('N'))] = int(asn)

    # load city info
    city_geo = list()
    city_list = list()
    city_lookup = dict()
    pos = 0
    with open(city_fpath, 'r') as f:
        next(f)
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            city, state, country, lat, lon = next(reader)
            city_geo.append([float(lat), float(lon)])
            city_list.append((city, state, country))
            city_lookup[(city, state, country)] = pos
            pos += 1
    # construct city coordinates tree to speed up mapping query
    city_geo = np.array(city_geo, dtype=np.double)
    city_tree = KDTree(city_geo)

    # use city as node in the graph
    G.add_nodes_from(list(city_lookup.values()))

    # load landcable info and use it as edges in the graph
    idx = 0
    with open(landcable_fpath, 'r') as f:
        next(f)
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            from_city, from_state, from_country, to_city, to_state, to_country, distance_km, _1_, _2_ = next(
                reader)
            from_pos = city_lookup[(from_city, from_state, from_country)]
            to_pos = city_lookup[(to_city, to_state, to_country)]
            G.add_edge(from_pos, to_pos, key=f'L{idx}', weight=float(
                distance_km), type='landcable', cableid=idx)
            idx += 1

    # load submarinecable info and use it as edges in the graph
    # Version 1: ki3
    # - step 1: load landing points
    lpts_id2cityidx = dict()  # { key: landing point id, value: city index }
    nb_landing_pts = 0
    nb_mapped_lpts = 0
    with open(landing_pts_path, 'r') as f:
        landing_pts_list = json.load(f)
        for landing_pts_item in landing_pts_list:
            nb_landing_pts += 1
            # extract landing point's coordinates
            lp_item_geo = landing_pts_item['geometry']
            coordinates = lp_item_geo.split('coordinates=')[1].split(']')[
                0].strip('[').split(',')
            coordinates = [float(x.strip()) for x in coordinates]
            coordinates.reverse()
            assert len(coordinates) == 2
            _, city_index = city_tree.query(coordinates)
            distance = calc_point_distance(
                np.array(coordinates), city_geo[city_index])
            if distance > 100:
                continue
            lp_item_properties = landing_pts_item['properties']
            id_value = lp_item_properties.split('id=')[1].split(',')[0]
            lpts_id2cityidx[id_value] = city_index
            nb_mapped_lpts += 1
    print("  Mapped {}/{} landing points".format(nb_mapped_lpts, nb_landing_pts))
    cable_geo_dict = dict()
    # - step 2: load cable geo
    with open(cable_geo_fpath, 'r') as f:
        cable_geo_list = json.load(f)
        for cable_geo_item in cable_geo_list:
            cable_id = cable_geo_item['properties'].split('id=')[
                1].split(',')[0]
            cable_geo = cable_geo_item['geometry']
            coordinates_str = cable_geo.split('coordinates=')[1].strip('}')
            coordinates = literal_eval(coordinates_str)
            if len(coordinates) != 0:
                cable_geo_dict[cable_id] = True
    # - step 3: load submarine cables
    idx = 0
    nb_submarine_cables = 0
    nb_submarine_cables_mapped = 0
    with open(submarinecable_fpath, 'r') as f:
        cable_list = json.load(f)['cables']
        for cable_item in cable_list:
            cable_id = cable_item['cableId']
            if cable_id not in cable_geo_dict:
                continue
            nb_submarine_cables += 1
            distance_km = cable_item['cableLength'][:-3]  # remove ' Km'
            lpts_id_list = list(
                map(lambda x: x['id'], cable_item['landPoints']))
            lpts_mapped_cityidx_list = [lpts_id2cityidx[landing_point_id]
                                        for landing_point_id in lpts_id_list if landing_point_id in lpts_id2cityidx]
            if len(lpts_mapped_cityidx_list) >= 2:
                nb_submarine_cables_mapped += 1
            sub_id = 0
            for i in range(len(lpts_mapped_cityidx_list)):
                for j in range(i+1, len(lpts_mapped_cityidx_list)):
                    from_pos = lpts_mapped_cityidx_list[i]
                    to_pos = lpts_mapped_cityidx_list[j]
                    G.add_edge(from_pos, to_pos, key=f'S{idx}-{sub_id}', weight=float(
                        distance_km), type='submarinecable', cableid=idx)
                    sub_id += 1
            idx += 1
    print("  Mapped {}/{} submarine cables".format(nb_submarine_cables_mapped,
          nb_submarine_cables))

    # load node to city index mapping
    node2cityidx = dict()
    nb_nodes = 0
    nb_nodes_mapped = 0
    with open(node_city_fpath, 'r') as f:
        for line in f:
            starter, nid, cityidx, distance = line.strip().split()
            assert starter == 'node.City'
            nb_nodes += 1
            if float(distance) > 80:
                continue
            nb_nodes_mapped += 1
            node2cityidx[int(nid.strip('N'))] = int(cityidx.strip('C'))
    print("  Mapped {}/{} nodes to city".format(nb_nodes_mapped, nb_nodes))

    # map link to cable
    idx = 0
    nb_links = 0
    nb_links_mapped = 0
    with open(link_fpath, 'r') as ifp, open(link_cable_fpath, 'w') as ofp:
        writer = csv.writer(ofp, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
        writer.writerow(['link_id', 'src_nid', 'dst_nid', 'src_asn',
                        'dst_asn', 'link_type', 'landcable_ids', 'submarine_ids'])
        for line in ifp:
            starter, idx, src_nid, dst_nid, ltype = line.strip().split()
            assert starter == 'link'
            nb_links += 1
            src_city_idx = node2cityidx.get(int(src_nid.strip('N')))
            dst_city_idx = node2cityidx.get(int(dst_nid.strip('N')))
            src_asn = node2as.get(int(src_nid.strip('N')))
            dst_asn = node2as.get(int(dst_nid.strip('N')))
            if src_city_idx is not None and dst_city_idx is not None:
                try:
                    path = nx.shortest_path(
                        G, source=src_city_idx, target=dst_city_idx, weight='weight')
                    cable_info_list = list()
                    for u, v in zip(path[:-1], path[1:]):
                        edge_data = G.get_edge_data(u, v)
                        min_weight_edge = min(
                            edge_data.values(), key=lambda x: x['weight'])
                        cable_info_list.append(min_weight_edge)
                    landcable_id_list = [cable_info.get(
                        'cableid') for cable_info in cable_info_list if cable_info.get('type') == 'landcable']
                    submarinecable_id_list = [cable_info.get(
                        'cableid') for cable_info in cable_info_list if cable_info.get('type') == 'submarinecable']
                    if len(submarinecable_id_list) > 0:
                        ltype = "Submarine-cable"
                    elif ltype == "Others":
                        ltype = "Direct"
                    else:
                        ltype = "IXP"
                    lcbl_str = ",".join([str(cable_id)
                                        for cable_id in landcable_id_list])
                    scbl_str = ",".join([str(cable_id)
                                        for cable_id in submarinecable_id_list])
                    writer.writerow(
                        [idx.strip("L:"), src_nid, dst_nid, src_asn, dst_asn, ltype, lcbl_str, scbl_str])
                    nb_links_mapped += 1
                except:
                    continue
    print("  Mapped {}/{} links".format(nb_links_mapped, nb_links))


# def map_link2cable(link_fpath, node_mapping, standard_path, facility_path, city_path, link_mapping):
#     # load facility info
#     facility_info = list()
#     with open(facility_path, 'r') as f:
#         for line in f:
#             line = StringIO(line.strip())
#             reader = csv.reader(line, delimiter=',', quotechar='"')
#             org, name, lat, lon, city, state, country, source, asof_date = next(reader)
#             try:
#                 coord = [float(lat), float(lon)]
#             except:
#                 continue
#             facility_info.append([city, state, country])
#     # load node to facility mapping
#     node2fac = dict()
#     node2city = dict()
#     with open(node_mapping, 'r') as f:
#         for line in f:
#             starter, nid, fid, distance = line.strip().split()
#             assert starter == 'node.Facility'
#             if 'F' in fid:
#                 node2fac[int(nid.strip('N'))] = int(fid.strip('F'))
#             elif 'C' in fid:
#                 node2city[int(nid.strip('N'))] = int(fid.strip('C'))
#     # load city info
#     city2pos = dict()
#     city_list = list()
#     pos = 0
#     with open(city_path, 'r') as f:
#         next(f)
#         for line in f:
#             line = StringIO(line.strip())
#             reader = csv.reader(line, delimiter=',', quotechar='"')
#             city, state, country, lat, lon = next(reader)
#             city_list.append(pos)
#             city2pos[(city, state, country)] = pos
#             pos += 1
#     # construct the graph
#     G = nx.Graph()
#     G.add_nodes_from(city_list)
#     # add standard paths as edges
#     nb_path = 0
#     with open(standard_path, 'r') as f:
#         next(f)
#         for line in f:
#             line = StringIO(line.strip())
#             reader = csv.reader(line, delimiter=',', quotechar='"')
#             from_city, from_state, from_country, to_city, to_state, to_country, distance_km, path_wkt, asof_date = next(reader)
#             distance = float(distance_km)
#             from_pos = city2pos[(from_city, from_state, from_country)]
#             to_pos = city2pos[(to_city, to_state, to_country)]
#             G.add_edge(from_pos, to_pos, weight=distance, pos=nb_path)
#             nb_path += 1

#     def get_city_idx(_id):
#         _id = int(_id.strip('N'))
#         if node2city.get(_id) is not None:
#             return node2city[_id]
#         elif node2fac.get(_id) is not None:
#             return city2pos[tuple(facility_info[node2fac[_id]])]
#         else:
#             return -1

#     # calculate shortest path for links
#     nb_link = 0
#     nb_mapped = 0
#     with open(link_fpath, 'r') as file1, open(link_mapping, 'w') as file2:
#         next(file1)
#         for line in file1:
#             nb_link += 1
#             starter, idx, src_nid, dst_nid, ltype = line.strip().split()
#             assert starter == 'link'
#             src_city_idx = get_city_idx(src_nid)
#             dst_city_idx = get_city_idx(dst_nid)
#             try:
#                 cable_id_list = None
#                 cable_id_list_string = "NULL"
#                 path = nx.shortest_path(G, source=src_city_idx, target=dst_city_idx, weight='weight')
#                 if len(path) > 1:
#                     cable_id_list = [G.get_edge_data(path[i], path[i+1]).get('pos') for i in range(len(path)-1)]
#                     cable_id_list_string = ' '.join([str(cable_id) for cable_id in cable_id_list])
#                 nb_mapped += 1
#                 content = "link {} {} {} {} {}\n".format(idx, src_nid, dst_nid, ltype, cable_id_list_string)
#                 file2.write(content)
#             except:
#                 continue
#     print('Mapped {}/{} links'.format(nb_mapped, nb_link))


def generate_pop_file(node_as_fpath, node_geo_fpath, node_facility_fpath, node_city_fpath, node_landing_pts_fpath, pop_fpath):
    print("Generating PoP file...")
    # load node to AS mapping
    node2as = dict()
    with open(node_as_fpath, 'r') as f:
        for line in f:
            _, nid, asn = line.strip().split()
            nid = int(nid.strip('N'))
            node2as[nid] = int(asn)

    # load node to geo mapping
    node2geo = dict()
    with open(node_geo_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            items = line.strip().split('\t')
            nid = int(items[0].split()[-1].strip('N:'))
            lat, lon = items[5:7]
            node2geo[nid] = [round(float(lat), KEEP_DIGIT_DIM), round(
                float(lon), KEEP_DIGIT_DIM)]

    # node to distance mapping
    # if facility exists, it is the distance between node and facility
    # if facility does not exist, it is the distance between node and city
    node2dis = dict()

    # load node to facility mapping
    node2fac = dict()
    with open(node_facility_fpath, 'r') as f:
        for line in f:
            starter, nid, fid, distance = line.strip().split()
            assert starter == 'node.Facility'
            distance = round(float(distance), KEEP_DIGITS_DIS)
            key = int(nid.strip('N'))
            value = int(
                fid.strip('F')) if distance < MAPPING_FAC_DISTANCE else -1
            node2fac[key] = value
            node2dis[key] = distance

    # load node to city mapping
    node2city = dict()
    with open(node_city_fpath, 'r') as f:
        for line in f:
            starter, nid, cid, distance = line.strip().split()
            assert starter == 'node.City'
            distance = round(float(distance), KEEP_DIGITS_DIS)
            key = int(nid.strip('N'))
            value = int(
                cid.strip('C')) if distance < MAPPING_CITY_DISTANCE else -1
            node2city[key] = value
            node2dis[key] = min(node2dis[key], distance)

    # load node to landing points mapping
    node2landing = dict()
    with open(node_landing_pts_fpath, 'r') as f:
        for line in f:
            starter, nid, lid, distance = line.strip().split()
            assert starter == 'node.landing_points'
            distance = round(float(distance), KEEP_DIGITS_DIS)
            key = int(nid.strip('N'))
            value = int(lid.strip('LP')
                        ) if distance < MAPPING_LANDING_PTS_DISTANCE else -1
            node2landing[key] = value

    # generate pop file
    with open(pop_fpath, 'w') as f:
        f.write("idx,asn,lat,lon,facility_id,city_id,landing_id,distance\n")
        writer = csv.writer(f, delimiter=',', quotechar='"',
                            quoting=csv.QUOTE_MINIMAL)
        for nid in sorted(node2as.keys()):
            asn = node2as[nid]
            lat, lon = node2geo[nid]
            facility_id = node2fac[nid]
            city_id = node2city[nid]
            landing_points_id = node2landing[nid]
            distance = node2dis[nid]
            content = [nid, asn, lat, lon, facility_id,
                       city_id, landing_points_id, distance]
            writer.writerow(content)


def transform_lpts_from_ki3(city_fpath, ki3_landing_pts_fpath, igdb_landing_pts_fpath):
    TODAY_STR = datetime.now().strftime('%Y-%m-%d')

    # load city info
    city_geo = list()
    city_label = list()
    with open(city_fpath, 'r') as f:
        next(f)
        city_dict = dict()
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            city, state, country, lat, lon = next(reader)
            city_geo.append([float(lat), float(lon)])
            city_label.append([city, state, country])
    city_geo = np.array(city_geo, dtype=np.double)
    tree = KDTree(city_geo)

    with open(ki3_landing_pts_fpath, 'r') as f:
        landing_pts_list = json.load(f)
        with open(igdb_landing_pts_fpath, 'w') as ofp:
            writer = csv.writer(ofp, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['city_name', 'state_province', 'country', 'latitude', 'longitude',
                            'source', 'asof_date', 'standard_city', 'standard_state', 'standard_country'])
            for landing_pts_item in landing_pts_list:
                lp_item_geo = landing_pts_item['geometry']
                coordinates = lp_item_geo.split('coordinates=')[1].split(']')[
                    0].strip('[').split(',')
                coordinates = [float(x.strip()) for x in coordinates]
                coordinates.reverse()
                assert len(coordinates) == 2
                lp_item_properties = landing_pts_item['properties']
                location_list = lp_item_properties.split(
                    'name=')[1].split(', is_tbd=')[0].split(',')
                if len(location_list) == 2:
                    city, country = location_list
                    state = ''
                elif len(location_list) == 3:
                    city, state, country = location_list
                else:
                    print(location_list)
                    raise Exception("Invalid location list")
                _, city_idx = tree.query(coordinates)
                standard_city, standard_state, standard_country = city_label[city_idx]
                writer.writerow([city, state, country, coordinates[0], coordinates[1],
                                'KI3', TODAY_STR, standard_city, standard_state, standard_country])


def transform_submarine_cable_from_ki3(submarinecable_fpath, cable_geo_fpath, igdb_submarinecable_fpath):
    TODAY_STR = datetime.now().strftime('%Y-%m-%d')

    coords_dict = dict()
    with open(cable_geo_fpath, 'r') as fp:
        cable_geo_list = json.load(fp)
        for cable_geo in cable_geo_list:
            feature_id = cable_geo['properties'].split('id=')[1].split(',')[0]
            coords = cable_geo['geometry'].split("coordinates=")[1].strip('}')
            coords = literal_eval(coords)
            coords = [[tuple(item) for item in sublist] for sublist in coords]
            coords_dict[feature_id] = to_wkt_multilinestring(coords)

    with open(submarinecable_fpath, 'r') as f:
        cable_list = json.load(f)['cables']
        print(len(cable_list))
        with open(igdb_submarinecable_fpath, 'w') as ofp:
            writer = csv.writer(ofp, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(['CABLE_ID', 'CABLE_NAME',
                            'FEATURE_ID', 'CABLE_WKT', 'SOURCE', 'ASOF_DATE'])
            for cable_item in cable_list:
                cable_id = cable_item['cableId']
                cable_name = cable_item['cableName']
                feature_id = cable_item['cableId']
                cable_wkt = coords_dict.get(cable_id)
                writer.writerow(
                    [cable_id, cable_name, feature_id, cable_wkt, 'KI3', TODAY_STR])


# def generate_pop_file(node_as_fpath, node_geo_fpath, node_map_path, pop_file):
#     # load node to AS mapping
#     node2as = dict()
#     with open(node_as_fpath, 'r') as f:
#         for line in f:
#             if line.startswith('#'):
#                 continue
#             _, nid, asn = line.strip().split()
#             nid = int(nid.strip('N'))
#             node2as[nid] = int(asn)
#     # load node to geo mapping
#     node2geo = dict()
#     with open(node_geo_fpath, 'r') as f:
#         for line in f:
#             if line.startswith('#'):
#                 continue
#             items = line.strip().split('\t')
#             nid = int(items[0].split()[-1].strip('N:'))
#             lat, lon = items[5:7]
#             node2geo[nid] = [round(float(lat), KEEP_DIGIT_DIM), round(float(lon), KEEP_DIGIT_DIM)]
#     # load node to facility/city mapping. Todo: landing points mapping
#     node2fac = dict()
#     node2city = dict()
#     node2fac_distance = dict()
#     with open(node_map_path, 'r') as f:
#         for line in f:
#             starter, nid, fid, distance = line.strip().split()
#             assert starter == 'node.Facility'
#             if 'F' in fid:
#                 node2fac[int(nid.strip('N'))] = int(fid.strip('F'))
#                 node2city[int(nid.strip('N'))] = -1
#             elif 'C' in fid:
#                 node2city[int(nid.strip('N'))] = int(fid.strip('C'))
#                 node2fac[int(nid.strip('N'))] = -1
#             else:
#                 node2fac[int(nid.strip('N'))] = -1
#                 node2city[int(nid.strip('N'))] = -1
#             node2fac_distance[int(nid.strip('N'))] = round(float(distance), KEEP_DIGITS_DIS)
#     # generate pop file
#     with open(pop_file, 'w') as f:
#         f.write("idx,asn,lat,lon,facility_id,city_id,landing_id,distance\n")
#         writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#         for nid in sorted(node2as.keys()):
#             asn = node2as[nid]
#             lat, lon = node2geo[nid]
#             facility_id = node2fac[nid]
#             city_id = node2city[nid]
#             landing_points_id = -1
#             distance = node2fac_distance[nid]
#             content = [nid, asn, lat, lon, facility_id, city_id, landing_points_id, distance]
#             writer.writerow(content)


# def map_pop_to_facility_and_city(city_file, polygon_file, facility_file, node_geo, node_mapping):
#     # load city info
#     city_info = list()
#     city_list = list()
#     with open(city_file, 'r') as f:
#         next(f)
#         for line in f:
#             line = StringIO(line.strip())
#             reader = csv.reader(line, delimiter=',', quotechar='"')
#             city, state, country, lat, lon = next(reader)
#             city_list.append([float(lat), float(lon)])
#             city_info.append([city, state, country])
#     city_list = np.array(city_list, dtype=np.double)
#     city_tree = KDTree(city_list)
#     # load polygon info
#     city2polygon = dict()
#     with open(polygon_file, 'r') as f:
#         next(f)
#         for line in f:
#             line = StringIO(line.strip())
#             reader = csv.reader(line, delimiter=',', quotechar='"')
#             city, state, country, polygon_wkt = next(reader)
#             polygon = loads(polygon_wkt)
#             city2polygon[(city, state, country)] = polygon
#     # load facility info
#     facility_info = list()
#     facility_coord = list()
#     with open(facility_file, 'r') as f:
#         next(f)
#         for line in f:
#             line = StringIO(line.strip())
#             reader = csv.reader(line, delimiter=',', quotechar='"')
#             org, name, lat, lon, city, state, country, source, asof_date = next(reader)
#             try:
#                 coord = [float(lat), float(lon)]
#                 facility_coord.append(coord)
#                 facility_info.append([city, state, country])
#             except:
#                 continue
#     facility_coord = np.array(facility_coord, dtype=np.double)
#     facility_tree = KDTree(facility_coord)
#     # load node geo info
#     node2geo = dict()
#     with open(node_geo, 'r') as f:
#         for line in f:
#             if line.startswith('#'):
#                 continue
#             items = line.strip().split('\t')
#             nid = int(items[0].split()[-1].strip('N:'))
#             lat, lon = items[5:7]
#             node2geo[nid] = np.array([float(lat), float(lon)], dtype=np.double)
#     nb_all = 0
#     nb_matched = 0
#     node2fac = dict()
#     node2dis = dict()
#     distances_fac = list()
#     distances_city = list()
#     MAPPING_FAC_DISTANCE = 100
#     MAPPING_CITY_DISTANCE = 200
#     for nid in node2geo.keys():
#         nb_all += 1
#         geo = node2geo[nid]
#         _, facility_id = facility_tree.query(geo)
#         _, city_id = city_tree.query(geo)
#         city1, state1, country1 = facility_info[facility_id]
#         city2, state2, country2 = city_info[city_id]
#         distance_fac = calc_point_distance(geo, facility_coord[facility_id])
#         distance_city = calc_point_distance(geo, city_list[city_id])
#         # First priority: map pop to facility
#         if city1 == city2 and state1 == state2 and country1 == country2 and distance_fac < MAPPING_FAC_DISTANCE:
#             nb_matched += 1
#             node2fac[nid] = "F{}".format(facility_id)
#             node2dis[nid] = round(distance_fac, KEEP_DIGITS_DIS)
#             distances_fac.append(distance_fac)
#         elif distance_fac < MAPPING_FAC_DISTANCE:
#             nb_matched += 1
#             node2fac[nid] = "F{}".format(facility_id)
#             node2dis[nid] = round(distance_fac, KEEP_DIGITS_DIS)
#             distances_fac.append(distance_fac)
#         # Second priority: map pop to city
#         elif distance_city < MAPPING_CITY_DISTANCE:
#             nb_matched += 1
#             node2fac[nid] = "C{}".format(city_id)
#             node2dis[nid] = round(distance_city, KEEP_DIGITS_DIS)
#             distances_city.append(distance_city)
#         # Third priority: see pop as independent node
#         else:
#             node2fac[nid] = "NULL".format(city_id)
#             node2dis[nid] = round(min(distance_fac, distance_city), KEEP_DIGITS_DIS)
#     with open(node_mapping, 'w') as f:
#         for nid, fac in node2fac.items():
#             f.write("node.Facility N{} {} {}\n".format(nid, fac, node2dis[nid]))
#     print('Matched {}/{} nodes'.format(nb_matched, nb_all))
#     for pcnt in range(0, 105, 5):
#         print('Percentile {}\t{}\t{}\t'.format(pcnt, np.percentile(distances_fac, pcnt), np.percentile(distances_city, pcnt)))


# def generate_link_file(link_mapping, node_node_as_fpath, link_fpath):
#     with open(node_node_as_fpath, 'r') as f:
#         node2as = dict()
#         for line in f:
#             if line.startswith('#'):
#                 continue
#             _, nid, asn = line.strip().split()
#             node2as[int(nid.strip('N'))] = int(asn)
#     with open(link_mapping, 'r') as file1, open(link_fpath, 'w') as file2:
#         writer = csv.writer(file2, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
#         writer.writerow(['link_id', 'src_nid', 'dst_nid', 'src_asn', 'dst_asn', 'type', 'cables'])
#         for line in file1:
#             items = line.strip().split()
#             starter = items[0]
#             assert starter == 'link'
#             idx, src_nid, dst_nid, ltype = items[1:5]
#             idx = idx.strip('L:')
#             src_asn = node2as[int(src_nid.strip('N'))]
#             dst_asn = node2as[int(dst_nid.strip('N'))]
#             writer.writerow([idx, src_nid, dst_nid, src_asn, dst_asn, ltype, ",".join(items[5:])])


# def get_dist_of_customer_cone(asrank_path):
#     nb_all = 0
#     nb_has_cone = 0
#     cone_sizes = list()
#     with open(asrank_path, 'r') as ifp:
#         for line in ifp:
#             asinfo = json.loads(line)
#             nb_all += 1
#             try:
#                 cone_sizes.append(int(asinfo['cone']['numberAsns']))
#                 nb_has_cone += 1
#             except:
#                 continue
#     print('Total ASes: {}, ASes with customer cone: {}'.format(nb_all, nb_has_cone))
#     cone_sizes.sort()
#     # cone_sizes = cone_sizes[-int(len(cone_sizes) * 0.0025):]
#     cone_sizes = np.array(cone_sizes, dtype=np.int32)
#     cone_sizes = np.log(cone_sizes) + np.ones(cone_sizes.shape)
#     for pcnt in range(0, 105, 5):
#         print('Percentile {}\t{}\t'.format(pcnt, np.percentile(cone_sizes, pcnt)))


# def stat_node_with_multiple_neighbor_per_astuple(link_fpath, node_node_as_fpath):
#     node2as = dict()
#     with open(node_node_as_fpath, 'r') as f:
#         for line in f:
#             if line.startswith('#'):
#                 continue
#             _, nid, asn = line.strip().split()
#             node2as[int(nid.strip('N'))] = int(asn)

#     node_astuple_map = defaultdict(int)
#     with open(link_fpath, 'r') as f:
#         for line in f:
#             if line.startswith('#'):
#                 continue
#             starter, link_id, src_node_id, dst_node_id, link_type = line.strip().split()
#             src_node_id = int(src_node_id.strip('N'))
#             dst_node_id = int(dst_node_id.strip('N'))
#             src_asn = node2as[src_node_id]
#             dst_asn = node2as[dst_node_id]
#             node_astuple_map[(src_asn, dst_asn, src_node_id)] += 1
#             node_astuple_map[(src_asn, dst_asn, dst_node_id)] += 1
#     node_astuple_degree = list(sorted(node_astuple_map.items(), key=lambda x: x[1], reverse=True))
#     for i, (key, value) in enumerate(node_astuple_degree):
#         if i > 100:
#             break
#         src_asn, dst_asn, node_id = key
#         print('ASN: {} -> ASN: {}, Node: {}, Degree: {}'.format(src_asn, dst_asn, node_id, value))
#     node_astuple_degree = list(sorted(node_astuple_map.values()))
#     # node_astuple_degree = node_astuple_degree[-int(len(node_astuple_degree) * 0.0025):]
#     step = 2
#     print(len(node_astuple_degree))
#     for pcnt in range(0, 100+step, step):
#         print('Percentile {}\t{}\t'.format(pcnt, np.percentile(node_astuple_degree, pcnt)))


def count_nb_of_ases_in_interdomain_links(link_fpath, node_as_fpath):
    node2as = dict()
    with open(node_as_fpath, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            _, nid, asn = line.strip().split()
            node2as[int(nid.strip('N'))] = int(asn)

    as_dict = dict()
    with open(link_fpath, 'r') as f:
        for line in f:
            starter, link_id, src_node_id, dst_node_id, link_type = line.strip().split()
            src_asn = node2as[int(src_node_id.strip('N'))]
            dst_asn = node2as[int(dst_node_id.strip('N'))]
            as_dict[src_asn] = True
            as_dict[dst_asn] = True
    print('Total ASes: {}'.format(len(as_dict)))


def count_nb_of_links_between_as_tuple(link_cable_fpath):
    as_tuple_dict = defaultdict(int)
    with open(link_cable_fpath, 'r') as f:
        next(f)
        for line in f:
            line = StringIO(line.strip())
            reader = csv.reader(line, delimiter=',', quotechar='"')
            link_id, src_nid, dst_nid, src_asn, dst_asn, link_type, landcable_ids, submarine_ids = next(
                reader)
            as_tuple_dict[(src_asn, dst_asn)] += 1
    for as_tuple, count in sorted(as_tuple_dict.items(), key=lambda x: x[1], reverse=True):
        print('AS{}-AS{} Count: {}'.format(as_tuple[0], as_tuple[1], count))


def analyze_city_mapping_with_high_distance(node_city_fpath):
    node_distances = list()
    with open(node_city_fpath, 'r') as f:
        for line in f:
            starter, nid, cityidx, distance = line.strip().split()
            assert starter == 'node.City'
            distance = float(distance)
            node_distances.append([nid, cityidx, distance])
    node_distances.sort(key=lambda x: x[2], reverse=True)
    for i in range(100):
        print("node.City {} {} Distance: {}".format(
            node_distances[i][0], node_distances[i][1], round(node_distances[i][2], 4)))


if __name__ == "__main__":
    itdk_base_dir = '/home/maq18/Projects/Data/ITDK/Base'
    iGDB_dir = '/home/maq18/Projects/Data/iGDB'
    servloc_dir = '/home/maq18/Projects/Data/ServLoc'

    node_as_fname = 'nodes.as'
    node_geo_fname = 'nodes.geo'
    link_fname = 'links'
    # extract_interdomain_links(os.path.join(itdk_base_dir, 'nodes.as'), os.path.join(itdk_base_dir, 'nodes.geo'), os.path.join(itdk_base_dir, 'links'))
    itdk_tmp_dir = itdk_base_dir.replace('Base', 'Tmp')
    # group_proximity_nodes(os.path.join(itdk_tmp_dir, node_as_fname), os.path.join(itdk_tmp_dir, node_geo_fname), os.path.join(itdk_tmp_dir, link_fname))
    itdk_target_dir = itdk_base_dir.replace('Base', 'Target')
    # remove_redundant_links(os.path.join(itdk_target_dir, link_fname), os.path.join(itdk_target_dir, node_as_fname))

    facility_fpath = os.path.join(iGDB_dir, 'phys_nodes/phy_nodes.csv')
    city_fpath = os.path.join(iGDB_dir, 'city_points/city_points.csv')
    # complete_phynode_city_info(city_fpath, facility_fpath)
    landcable_fpath = os.path.join(
        iGDB_dir, 'standard_paths/InternetAtlas_standard_paths.csv')
    # simplify_line_string(landcable_fpath, city_fpath)

    node_geo_fpath = os.path.join(itdk_target_dir, node_geo_fname)
    complete_facility_fpath = os.path.join(
        iGDB_dir, 'phys_nodes/phy_nodes_complete.csv')
    node_facility_fpath = os.path.join(itdk_target_dir, 'nodes.Facility')

    # map_pop_to_facility(node_geo_fpath, complete_facility_fpath, node_facility_fpath)
    node_city_fpath = os.path.join(itdk_target_dir, 'nodes.City')
    # map_pop_to_city(node_geo_fpath, city_fpath, node_city_fpath)
    landing_points_fpath = os.path.join(servloc_dir, 'landing_point_geo.json')
    node_landing_pts_fpath = os.path.join(
        itdk_target_dir, 'nodes.landing_points')
    # map_pop_to_landing_points(node_geo_fpath, landing_points_fpath, node_landing_pts_fpath)

    # analyze_facility_mapping_distance(node_facility_fpath)
    # analyze_city_mapping_distance(node_city_fpath)

    unique_link_fpath = os.path.join(itdk_target_dir, 'unique_links')
    node_as_fpath = os.path.join(itdk_target_dir, 'nodes.as')
    simplified_landcable_fpath = os.path.join(
        iGDB_dir, 'standard_paths/simplified_InternetAtlas_standard_paths.csv')
    submarine_cable_fpath = os.path.join(servloc_dir, 'submarine_cables.json')
    link_cable_fpath = os.path.join(itdk_target_dir, 'unique_links_cable.csv')
    cable_geo_fpath = os.path.join(servloc_dir, 'cable_geo.json')
    # map_link2cable(unique_link_fpath, node_as_fpath, node_city_fpath, city_fpath, simplified_landcable_fpath, submarine_cable_fpath, landing_points_fpath, cable_geo_fpath, link_cable_fpath)
    # generate_pop_file(node_as_fpath, node_geo_fpath, node_facility_fpath, node_city_fpath, node_landing_pts_fpath, os.path.join(itdk_target_dir, 'pop.csv'))
    # transform_lpts_from_ki3(city_fpath, landing_points_fpath, os.path.join(servloc_dir, 'landing_point_geo.csv'))
    # transform_submarine_cable_from_ki3(submarine_cable_fpath, os.path.join(servloc_dir, 'cable_geo.json'), os.path.join(servloc_dir, 'ki3_submarine_cables.csv'))
    # count_nb_of_ases_in_interdomain_links(unique_link_fpath, node_as_fpath)
    # count_nb_of_links_between_as_tuple(link_cable_fpath)
    analyze_city_mapping_with_high_distance(node_city_fpath)
