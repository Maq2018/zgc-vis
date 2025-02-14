import json
import logging
import traceback
from pydantic import BaseModel
from datetime import datetime
from utils.conversion import to_float, split_string, parse_wkt_multilinestring, parse_wkt_linestring



logger = logging.getLogger("asn.models")
KEEP_DIGITS = 4
DATE_FORMAT = "%Y-%m-%d"


class VisPhysicalNode(BaseModel):
    index: int
    name: str
    organization: str
    latitude: float
    longitude: float
    city: str
    state: str
    country: str
    source: str
    date: datetime

    @classmethod
    def from_line(cls, line: str):
        keys = ['organization', 'name', "latitude", "longitude", "city", "state", "country", "source", "date"]
        try:
            item = dict(zip(keys, split_string(line)))
            assert len(item) == 9
            _organization = item['organization'].strip("\"")
            _name = item['name'].strip("\"")
            _latitude = to_float(item['latitude'])
            _longitude = to_float(item['longitude'])
            _city = item['city']
            _state = item['state']
            _country = item['country']
            _source = item['source']
            _date = datetime.strptime(item['date'], DATE_FORMAT)
            obj = {
                "name": _name,
                "organization": _organization,
                "latitude": _latitude,
                "longitude": _longitude,
                "city": _city,
                "state": _state,
                "country": _country,
                "source": _source,
                "date": _date
            }
            return obj
        except Exception as e:
            logger.error('Fail when processing line: %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
        

class VisSubmarineCable(BaseModel):
    id: str
    name: str
    feature_id: str
    coordinates: list
    source: str
    date: datetime

    @classmethod
    def from_line(cls, line:str):
        keys = ['id', 'name', 'feature_id', 'coordinates', 'source', 'date']
        try:
            item = dict(zip(keys, split_string(line)))
            assert len(item) == 6
            wkt = item['coordinates'].strip("\"")
            assert wkt.startswith("MULTILINESTRING")
            _id = item['id']
            _name = item['name']
            _feature_id = item['feature_id']
            _coordinates = parse_wkt_multilinestring(wkt)
            _source = item['source']
            _date = datetime.strptime(item['date'], DATE_FORMAT)
            obj = {
                "id": _id,
                "name": _name,
                "feature_id": _feature_id,
                "coordinates": _coordinates,
                "source": _source,
                "date": _date
            }
            return obj
        except Exception as e:
            logger.error('Fail when processing line: %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
        

class VisLandingPoint(BaseModel):
    index: int
    cable_id: str
    active: bool
    latitude: float
    longitude: float
    city: str
    state: str
    country: str
    source: str
    date: datetime

    @classmethod
    def from_line(cls, line: str):
        # Todo: load landing point from file with complete information
        keys = ["city_name", "state_province", "country", "latitude", "longitude", "source", "asof_date", "standard_city", "standard_state", "standard_country"]
        try:
            item = dict(zip(keys, split_string(line)))
            assert len(item) == 10
            _latitude = to_float(item['latitude'])
            _longitude = to_float(item['longitude'])
            _city = item['standard_city']
            _state = item['standard_state']
            _country = item['standard_country']
            _source = item['source']
            _date = datetime.strptime(item['asof_date'], DATE_FORMAT)
            obj = {
                "latitude": _latitude,
                "longitude": _longitude,
                "city": _city,
                "state": _state,
                "country": _country,
                "source": _source,
                "date": _date
            }
            return obj
        except Exception as e:
            logger.error('Fail when processing line: %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
        
    @classmethod
    def fill_unknown_fields(cls, obj, idx):
        obj['index'] = idx
        obj['cable_id'] = 'unknown'
        obj['active'] = True
        return obj
    

class VisLandCable(BaseModel):
    index: int
    from_city: str
    from_state: str
    from_country: str
    to_city: str
    to_state: str
    to_country: str
    distance: float
    coordinates: list
    date: datetime

    @classmethod
    def from_line(cls, line: str):
        keys = ['from_city', 'from_state', 'from_country', 'to_city', 'to_state', 'to_country', 'distance', 'coordinates', 'date']
        try:
            item = dict(zip(keys, split_string(line)))
            assert len(item) == 9
            wkt = item['coordinates'].strip("\"")
            assert wkt.startswith("LINESTRING")
            _from_city = item['from_city']
            _from_state = item['from_state']
            _from_country = item['from_country']
            _to_city = item['to_city']
            _to_state = item['to_state']
            _to_country = item['to_country']
            _distance = to_float(item['distance'])
            _coordinates = parse_wkt_linestring(wkt)
            _date = datetime.strptime(item['date'], DATE_FORMAT)
            obj = {
                "from_city": _from_city,
                "from_state": _from_state,
                "from_country": _from_country,
                "to_city": _to_city,
                "to_state": _to_state,
                "to_country": _to_country,
                "distance": _distance,
                "coordinates": _coordinates,
                "date": _date
            }
            return obj
        except Exception as e:
            logger.error('Fail when processing line: %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
        
    @classmethod
    def fill_unknown_fields(cls, obj, idx):
        obj['index'] = idx
        return obj
    

class VisPop(BaseModel):
    index: int
    asn: int
    latitude: float
    longitude: float
    facility_id: int
    city_id: int
    landing_point_id: int
    distance: float

    @classmethod
    def from_line(cls, line: str):
        keys = ['index', 'asn', 'latitude', 'longitude', 'facility_id', 'city_id', 'landing_point_id', 'distance']
        try:
            item = dict(zip(keys, split_string(line)))
            assert len(item) == 8
            _index = int(item['index'])
            _asn = int(item['asn'])
            _latitude = to_float(item['latitude'])
            _longitude = to_float(item['longitude'])
            _facility_id = int(item['facility_id'])
            _city_id = int(item['city_id'])
            _landing_point_id = int(item['landing_point_id'])
            _distance = to_float(item['distance'])
            obj = {
                "index": _index,
                "asn": _asn,
                "latitude": _latitude,
                "longitude": _longitude,
                "facility_id": _facility_id,
                "city_id": _city_id,
                "landing_point_id": _landing_point_id,
                "distance": _distance
            }
            return obj
        except Exception as e:
            logger.error('Fail when processing line: %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
        

class VisPhysicalLink(BaseModel):
    index: int
    src_pop_index: int
    dst_pop_index: int
    src_asn: int
    dst_asn: int
    ltype: str
    cable_ids: list

    @classmethod
    def from_line(cls, line: str):
        keys = ['index', 'src_pop_index', 'dst_pop_index', 'src_asn', 'dst_asn', 'ltype', 'cable_ids']
        try:
            item = dict(zip(keys, split_string(line)))
            assert len(item) == 7
            _index = int(item['index'])
            _src_pop_index = int(item['src_pop_index'].strip('N'))
            _dst_pop_index = int(item['dst_pop_index'].strip('N'))
            _src_asn = int(item['src_asn'])
            _dst_asn = int(item['dst_asn'])
            _ltype = item['ltype']
            _cable_ids = [int(cid) for cid in item['cable_ids'].strip().split(',')] if item['cable_ids'] != 'NULL' else []
            obj = {
                "index": _index,
                "src_pop_index": _src_pop_index,
                "dst_pop_index": _dst_pop_index,
                "src_asn": _src_asn,
                "dst_asn": _dst_asn,
                "ltype": _ltype,
                "cable_ids": _cable_ids
            }
            return obj
        except Exception as e:
            logger.error('Fail when processing line: %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None


class VisLogicNode(BaseModel):
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
    def to_obj(cls, idx, asn, asrank):
        try:
            organization_info = asrank.get('organization', None)
            organization = organization_info.get('orgName', '') if organization_info else ''
            country_info = asrank.get('country', None)
            country_name = country_info.get('name', '') if country_info else ''
            country_code = country_info.get('iso', '') if country_info else ''
            res = {
                "index": idx,
                "asn": int(asrank['asn']),
                "name": str(asrank.get('asnName', '')),
                "rank": int(asrank.get('rank', 10000000)),
                "organization": organization,
                "country": country_name,
                "country_code": country_code,
                "latitude": round(float(asrank['latitude']), KEEP_DIGITS),
                "longitude": round(float(asrank['longitude']), KEEP_DIGITS),
                "cone_size": int(asrank.get('cone', {}).get('numberAsns', 0)),
                "cone_prefix_size": int(asrank.get('cone', {}).get('numberPrefixes', 0)),
                "degree_provider": int(asrank.get('asnDegree', {}).get('provider', 0)),
                "degree_customer": int(asrank.get('asnDegree', {}).get('customer', 0)),
                "degree_peer": int(asrank.get('asnDegree', {}).get('peer', 0)),
                "prefix_size": int(asrank.get('announcing', {}).get("numberPrefixes", 0))
            }
            return res
        except Exception as e:
            logger.error(f'failed to generate obj for asrank with {asn},'
                         f' err: {e}, stack: {traceback.format_exc()}')
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

class VisCity(BaseModel):
    index: int
    city: str
    state: str
    country: str
    latitude: float
    longitude: float

    @classmethod
    def from_line(cls, line):
        keys = ['city', 'state', 'country', 'latitude', 'longitude']
        try:
            city_info = dict(zip(keys, split_string(line)))
            obj = {
                "city": city_info['city'],
                "state": city_info['state'],
                "country": city_info['country'],
                "latitude": round(float(city_info['latitude']), KEEP_DIGITS),
                "longitude": round(float(city_info['longitude']), KEEP_DIGITS)
            }
            return obj
        except Exception as e:
            logger.error('failed to generate obj for city with %s, err: %s, stack: %s', line, e, traceback.format_exc())
            return None
        
    @classmethod
    def fill_unknown_fields(cls, obj, idx):
        obj['index'] = idx
        return obj
