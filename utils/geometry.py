import numpy as np
import logging
from sklearn.cluster import DBSCAN
from asn.models import KEEP_DIGITS

MIN_CLUSTER_DISTANCE = 50 # km
logger = logging.getLogger("utils.geometry")
np.set_printoptions(precision=KEEP_DIGITS)


def to_pos(obj):
    try:
        return np.array([obj['latitude'], obj['longitude']], dtype=np.double)
    except KeyError:
        logger.error(f"to_pos: object {obj} does not have latitude and longitude")
        raise ValueError("object does not have latitude and longitude")


def calc_center_pos(pset, pos_info):
    if not pset:
        logger.error("calc_center_pos: empty point set")
        raise ValueError("empty point set")
    lat = np.mean([pos_info[p][0] for p in pset])
    lng = np.mean([pos_info[p][1] for p in pset])
    return np.round(np.array([lat, lng]), KEEP_DIGITS)


def calc_point_distance(pos1:np.array, pos2:np.array):# [lat1, lng1], [lat2, lng2]
    radius = np.double(6371) # km
    dlat, dlng = np.radians(pos2 - pos1)
    a = np.sin(dlat/2) * np.sin(dlat/2) + np.cos(np.radians(pos1[0])) * np.cos(np.radians(pos2[0])) * np.sin(dlng/2) * np.sin(dlng/2)
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))
    d = radius * c
    return np.round(d, KEEP_DIGITS)


def haversine_distance(pos1, pos2):
    radius=np.double(6371.0)
    lat1, lon1 = np.radians(pos1)
    lat2, lon2 = np.radians(pos2)
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    distance = radius * c
    return distance


def cluster_by_distance(idx_list, pos_info, min_distance=MIN_CLUSTER_DISTANCE):
    clusters = []
    for idx in idx_list:
        found = False
        for cluster in clusters:
            if calc_point_distance(pos_info[idx], pos_info[cluster[0]]) < min_distance:
                cluster.append(idx)
                found = True
                break
        if not found:
            clusters.append([idx])
    return clusters

def cluster_by_distance_dbscan(idx_list, pos_info, min_distance=MIN_CLUSTER_DISTANCE):
    # check if empty
    if len(idx_list) == 0:
        return []
    # check if only one point
    if len(idx_list) == 1:
        return [idx_list]
    # extract positions
    positions = np.array([pos_info[idx] for idx in idx_list])
    # use DBSCAN for clustering
    db = DBSCAN(eps=min_distance, min_samples=1, metric=calc_point_distance).fit(positions)
    # get labels
    labels = db.labels_
    # create clusters
    clusters = {}
    for idx, label in zip(idx_list, labels):
        if label not in clusters:
            clusters[label] = []
        clusters[label].append(idx)
    # return clusters
    return list(clusters.values())
