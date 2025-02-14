import logging
from fastapi import APIRouter, Depends
from database.models import TableSelector
from .query import (
    PhysicalNodeQuery, 
    SubmarineCableQuery,
    LandingPointQuery,
    LandCableQuery, 
    LogicLinkQuery, 
    LogicNodeQuery,
    PoPQuery,
    PhyLinkQuery,
    CityQuery
)


router = APIRouter(prefix='/servloc')
logger = logging.getLogger('asn.views')
NB_LOGIC_NODE_SAMPLE = 10000
NB_LOGIC_LINK_SAMPLE = 10000


@router.get('/physical-nodes/detail')
async def get_nodes(args: PhysicalNodeQuery = Depends()):
    try:
        _table = TableSelector.get_physical_nodes_table()
        # setting up query parameters
        fields = ['idxs', 'nms', 'orgs', 'cts', 'sts', 'cys', 'srs']
        columns = ['index', 'name', 'organization', 'city', 'state', 'country', 'source']
        query_params = dict()
        for i, field in enumerate(fields):
            params = getattr(args, field, None)
            if params:
                if i == 0:
                    query_params[columns[i]] = {'$in': [int(param) for param in params.split(',')]}
                else:
                    query_params[columns[i]] = {'$in': params.split(',')}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'Fail to get physical nodes with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    

@router.get('/submarine-cables/detail')
async def get_submarine_cables(args: SubmarineCableQuery = Depends()):
    try:
        _table = TableSelector.get_submarine_cables_table()
        fields = ['ids', 'nms', 'fids', 'srs']
        columns = ['id', 'name', 'feature_id', 'source']
        query_params = dict()
        for i, field in enumerate(fields):
            params = getattr(args, field, None)
            if params:
                query_params[columns[i]] = {'$in': params.split(',')}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'Fail to get submarine cables with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    

@router.get('/landing-points/detail')
async def get_landing_points(args: LandingPointQuery = Depends()):
    try:
        _table = TableSelector.get_landing_points_table()
        fields = ['idxs', 'cidxs', 'active', 'ctys', 'sts', 'cys', 'srs']
        columns = ['index', 'cable_id', 'active', 'city', 'state', 'country', 'source']
        query_params = dict()
        for i, field in enumerate(fields):
            params = getattr(args, field, None)
            if params:
                if i == 2:
                    query_params[columns[i]] = params == 'true'
                else:
                    query_params[columns[i]] = {'$in': params.split(',')}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'Fail to get landing points with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    

@router.get('/land-cables/detail')
async def get_land_cables(args: LandCableQuery = Depends()):
    try:
        _table = TableSelector.get_land_cables_table()
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'Fail to get land cables with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    

@router.get('/logic-nodes/detail')
async def get_logic_nodes(args: LogicNodeQuery = Depends()):
    try:
        _table = TableSelector.get_logic_nodes_table()
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        if args.asns:
            query_params['asn'] = {'$in': [int(asn) for asn in args.asns.split(',')]}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}).sort({'rank': 1}).limit(NB_LOGIC_NODE_SAMPLE):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get logic_nodes data with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}

    
@router.get('/logic-links/detail')
async def get_logic_links(args: LogicLinkQuery = Depends()):
    try:
        _table = TableSelector.get_logic_links_table()
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        elif args.asn:
            query_params['$or'] = [{'src_asn': int(args.asn)}, {'dst_asn': int(args.asn)}]
        elif args.asns:
            asns = [int(asn) for asn in args.asns.split(',')]
            query_params['$and'] = [{'src_asn': {'$in': asns}}, {'dst_asn': {'$in': asns}}]
        elif args.astuple:
            asn1, asn2 = map(int, args.astuple.strip().split(','))
            query_params['$or'] = [{'src_asn': asn1, 'dst_asn': asn2}, {'src_asn': asn2, 'dst_asn': asn1}]
        data = []
        async for cur in _table.find(query_params, {'_id': 0}).limit(NB_LOGIC_LINK_SAMPLE):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get logic_links data with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    
@router.get('/pop/detail')
async def get_pop(args: PoPQuery = Depends()):
    try:
        _table = TableSelector.get_pop_table()
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        if args.asns:
            query_params['asn'] = {'$in': [int(asn) for asn in args.asns.split(',')]}
        if args.fidxs:
            query_params['facility_id'] = {'$in': [int(fid) for fid in args.fidxs.split(',')]}
        if args.cidxs:
            query_params['city_id'] = {'$in': [int(cid) for cid in args.cidxs.split(',')]}
        if args.lidxs:
            query_params['landing_point_id'] = {'$in': [int(lid) for lid in args.lidxs.split(',')]}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get pop data, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    
@router.get('/phy-links/detail')
async def get_phy_links(args: PhyLinkQuery = Depends()):
    try:
        _table = TableSelector.get_phy_links_table()
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        elif args.pidxs:
            query_params['$or'] = [{'src_pop_index': {'$in': [int(pid) for pid in args.pidxs.split(',')]}}, {'dst_pop_index': {'$in': [int(pid) for pid in args.pidxs.split(',')]}}]
        elif args.asns:
            query_params['$or'] = [{'src_asn': {'$in': [int(asn) for asn in args.asns.split(',')]}}, {'dst_asn': {'$in': [int(asn) for asn in args.asns.split(',')]}}]
        elif args.astuple:
            asn1, asn2 = map(int, args.astuple.strip().split(','))
            query_params['$or'] = [{'src_asn': asn1, 'dst_asn': asn2}, {'src_asn': asn2, 'dst_asn': asn1}]
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get phy_links data, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    
@router.get('/city/detail')
async def get_city(args: CityQuery = Depends()):
    try:
        _table = TableSelector.get_city_table()
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get city data, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
