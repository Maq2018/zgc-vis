import logging
import json
from pydantic import BaseModel, Field
from pymongo import InsertOne, UpdateOne
from fastapi import Query
import traceback


logger = logging.getLogger("asn.models")
KEEP_DIGITS = 4


class VisNode(BaseModel):
    index: int
    asn: int
    country: str
    latitude: float
    longitude: float
    cluster_id: int

    @classmethod
    def from_line(cls, line):
        try:
            item_list = line.split(',')
            idx, asn, country, latitude, longitude, cluster_id = item_list
            assert asn.isdigit() and len(country)==2
            obj = {
                "index": int(idx),
                "asn": int(asn),
                "country": country,
                "latitude": round(float(latitude), KEEP_DIGITS),
                "longitude": round(float(longitude), KEEP_DIGITS),
                "cluster_id": int(cluster_id)
            }
            return obj
        except Exception as e:
            logger.error('failed to generate obj for node with %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
        

class VisCluster(BaseModel):
    index: int
    size: int
    latitude: float
    longitude: float

    @classmethod
    def from_line(cls, line):
        try:
            idx, size, latitude, longitude = line.split(',')
            obj = {
                "index": int(idx),
                "size": int(size),
                "latitude": round(float(latitude), KEEP_DIGITS),
                "longitude": round(float(longitude), KEEP_DIGITS)
            }
            return obj
        except Exception as e:
            logger.error('failed to generate obj for cluster with %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
    

class VisPhyLink(BaseModel):
    index: int
    src_asn: int
    dst_asn: int
    src_country: str
    dst_country: str
    src_nidx: int
    dst_nidx: int
    src_cidx: int
    dst_cidx: int
    src_latitude: float
    src_longitude: float
    dst_latitude: float
    dst_longitude: float
    link_type: str
    cross_cluster: bool
    distance: float

    @classmethod
    def from_line(cls, line):
        try:
            item_list = line.split(',')
            assert len(item_list) == 16
            idx, src_asn, dst_asn, src_country, dst_country, src_ndix, dst_nidx, src_cidx, dst_cidx, src_lat, src_lon, dst_lat, dst_lon, link_type, cross_cluster, distance = item_list
            obj = {
                "index": int(idx),
                "src_asn": int(src_asn),
                "dst_asn": int(dst_asn),
                "src_country": src_country,
                "dst_country": dst_country,
                "src_nidx": int(src_ndix),
                "dst_nidx": int(dst_nidx),
                "src_cidx": int(src_cidx),
                "dst_cidx": int(dst_cidx),
                "src_latitude": round(float(src_lat), KEEP_DIGITS),
                "src_longitude": round(float(src_lon), KEEP_DIGITS),
                "dst_latitude": round(float(dst_lat), KEEP_DIGITS),
                "dst_longitude": round(float(dst_lon), KEEP_DIGITS),
                "link_type": link_type,
                "cross_cluster": bool(int(cross_cluster)),
                "distance": round(float(distance), KEEP_DIGITS)
            }
            return obj
        except Exception as e:
            logger.error('failed to generate obj for phy_link with %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None


class VisLogicNode(BaseModel):
    index: int
    asn: int
    name: str
    organization: str
    country: str
    country_code: str
    latitude: float
    longitude: float
    rank: int

    @classmethod
    def to_obj(cls, idx, asn, asrank):
        try:
            obj = {
                "index": idx,
                "asn": asn,
                "name": asrank.get('asnName', ''),
                "organization": asrank.get('organization', {}).get('orgName', ''),
                "country": asrank.get('country', {}).get('name', ''),
                "country_code": asrank.get('country', {}).get('iso', ''),
                "latitude": round(float(asrank['latitude']), KEEP_DIGITS),
                "longitude": round(float(asrank['longitude']), KEEP_DIGITS),
                "rank": asrank.get('rank', 10000000)
            }
            return obj
        except Exception as e:
            logger.error('failed to generate obj for logic_node with %s, err: %s, stack: %s', str(asn), e, traceback.format_exc())
            return None


class VisLogicLink(BaseModel):
    index: int
    src_node_index: int
    dst_node_index: int
    src_asn: int
    dst_asn: int
    src_latitude: float
    src_longitude: float
    dst_latitude: float
    dst_longitude: float
    link_type: str

    @classmethod
    def to_obj(cls, idx, src_nidx, dst_nidx, link_type, src_asrank, dst_asrank):
        try:
            obj = {
                "index": idx,
                "src_node_index": src_nidx,
                "dst_node_index": dst_nidx,
                "src_asn": int(src_asrank['asn']),
                "dst_asn": int(dst_asrank['asn']),
                "src_latitude": round(float(src_asrank['latitude']), KEEP_DIGITS),
                "src_longitude": round(float(src_asrank['longitude']), KEEP_DIGITS),
                "dst_latitude": round(float(dst_asrank['latitude']), KEEP_DIGITS),
                "dst_longitude": round(float(dst_asrank['longitude']), KEEP_DIGITS),
                "link_type": link_type
            }
            return obj
        except Exception as e:
            logger.error('failed to generate obj for logic_link with %s, err: %s, stack: %s', str(idx), e, traceback.format_exc())
            return None


class VisASRank(BaseModel):
    index: int
    asn: int
    name: str
    rank: int
    organization: str
    country: str
    country_code: str
    latitude: float
    longitude: float
    cone_size: int
    cone_prefix_size: int
    degree_provider: int
    degree_customer: int
    degree_peer: int
    prefix_size: int

    @classmethod
    def from_line(cls, line, idx=0):
        try:
            obj = json.loads(line)
            latitude = obj.get('latitude', 0)
            longitude = obj.get('longitude', 0)
            organization_info = obj.get('organization', None)
            organization = organization_info.get('orgName', '') if organization_info else ''
            country_info = obj.get('country', None)
            country_name = country_info.get('name', '') if country_info else ''
            country_code = country_info.get('iso', '') if country_info else ''
            res = {
                "index": idx,
                "asn": int(obj['asn']),
                "name": str(obj.get('asnName', '')),
                "rank": int(obj.get('rank', 10000000)),
                "organization": organization,
                "country": country_name,
                "country_code": country_code,
                "latitude": round(float(latitude), KEEP_DIGITS),
                "longitude": round(float(longitude), KEEP_DIGITS),
                "cone_size": int(obj.get('cone', {}).get('numberAsns', 0)),
                "cone_prefix_size": int(obj.get('cone', {}).get('numberPrefixes', 0)),
                "degree_provider": int(obj.get('asnDegree', {}).get('provider', 0)),
                "degree_customer": int(obj.get('asnDegree', {}).get('customer', 0)),
                "degree_peer": int(obj.get('asnDegree', {}).get('peer', 0)),
                "prefix_size": int(obj.get('announcing', {}).get("numberPrefixes", 0))
            }
            return res
        except Exception as e:
            logger.error(f'failed to generate obj for asrank with {line},'
                         f' err: {e}, stack: {traceback.format_exc()}')
            return None


class NodeQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    asns: str = Field(Query(default=''))
    ctys: str = Field(Query(default=''))
    cidxs: str = Field(Query(default=''))


class ClusterQuery(BaseModel):
    idxs: str = Field(Query(default=''))


class ASRankQuery(BaseModel):
    asns: str = Field(Query(default=''))
    limit: int = Field(Query(default=10))


class PhyLinkQuery(BaseModel):
    nidxs: str = Field(Query(default=''))
    asns: str = Field(Query(default=''))
    ctys: str = Field(Query(default=''))
    ltypes: str = Field(Query(default=''))

class LogicLinkQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    asns: str = Field(Query(default=''))

class LogicNodeQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    asns: str = Field(Query(default=''))
    ctys: str = Field(Query(default=''))
