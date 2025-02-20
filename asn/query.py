from pydantic import BaseModel, Field
from fastapi import Query


class PhysicalNodeQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    nms: str = Field(Query(default='')) # nms: name
    orgs: str = Field(Query(default='')) # orgs: organization
    cts: str = Field(Query(default='')) # cts: city
    sts: str = Field(Query(default='')) # sts: state
    cys: str = Field(Query(default='')) # cys: country
    srs: str = Field(Query(default='')) # srs: source
    # Todo: add date

class SubmarineCableQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    ids: str = Field(Query(default=''))
    nms: str = Field(Query(default='')) # nms: name
    fids: str = Field(Query(default='')) # fids: feature_id
    srs: str = Field(Query(default='')) # srs: source
    # Todo: add date

class LandingPointQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    cidxs: str = Field(Query(default=''))
    active: str = Field(Query(default=''))
    ctys: str = Field(Query(default=''))
    sts: str = Field(Query(default=''))
    cys: str = Field(Query(default=''))
    srs: str = Field(Query(default=''))
    # Todo: add date

class LandCableQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    # Todo: add date

class LogicNodeQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    asns: str = Field(Query(default=''))

class LogicLinkQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    asn: str = Field(Query(default=''))
    asns: str = Field(Query(default=''))
    astuple: str = Field(Query(default=''))

class PoPQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    asns: str = Field(Query(default=''))
    fidxs: str = Field(Query(default='')) # facility_id(s)
    cidxs: str = Field(Query(default='')) # city_ids
    lidxs: str = Field(Query(default='')) # landing_point_ids

class PhyLinkQuery(BaseModel):
    idxs: str = Field(Query(default=''))
    pidxs: str = Field(Query(default='')) # pop_ids
    asns: str = Field(Query(default='')) # asns: asn
    astuple: str = Field(Query(default=''))

class CityQuery(BaseModel):
    idxs: str = Field(Query(default=''))
