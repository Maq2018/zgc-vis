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
