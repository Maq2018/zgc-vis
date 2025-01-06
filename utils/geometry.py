import numpy as np
import logging
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


def cluster_by_distance(idx_list, pos_info, min_distance=MIN_CLUSTER_DISTANCE):
    origin_set = set(idx_list)
    res = []
    while len(origin_set) > 0:
        new_set = set()
        new_set.add(origin_set.pop())
        remove_set = set()
        while True:
            pushd_in = False
            center_pos = calc_center_pos(new_set, pos_info)
            for idx in origin_set:
                if calc_point_distance(pos_info[idx], center_pos) < min_distance:
                    remove_set.add(idx)
                    pushd_in = True
            if not pushd_in:
                break
            origin_set.difference_update(remove_set)
            new_set.update(remove_set)
            remove_set.clear()
        res.append(new_set)
    return res
