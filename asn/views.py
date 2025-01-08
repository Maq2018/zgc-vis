from fastapi import APIRouter, Depends
from database.models import TableSelector
from .query import (
    PhysicalNodeQuery, 
    SubmarineCableQuery,
    LandingPointQuery,
    LandCableQuery,
    ASRankQuery, 
    PhyLinkQuery, 
    LogicLinkQuery, 
    ClusterQuery, 
    LogicNodeQuery
)
import logging


router = APIRouter(prefix='/servloc')
logger = logging.getLogger('asn.views')
NB_NODE_SAMPLE = 10000
NB_PHY_LINK_SAMPLE = 200000
NB_LOGIC_NODE_SAMPLE = 10000
NB_LOGIC_LINK_SAMPLE = 10000
MIN_DISTANCE = 100
MAX_DISTANCE = 8000


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
        async for cur in _table.find(query_params, {'_id': 0}).limit(500):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'Fail to get land cables with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}


@router.get('/nodes/summary/country')
async def get_nodes_summary_country():
    try:
        _table = TableSelector.get_nodes_table()
        pipe = [
            {'$group': {'_id': '$country', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]
        data = []
        async for cur in _table.aggregate(pipe):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get nodes summary with {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}

@router.get('/nodes/summary/asn')
async def get_nodes_summary_asn():
    try:
        _table = TableSelector.get_nodes_table()
        pipe = [
            {'$group': {'_id': '$asn', 'count': {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$limit': 10}
        ]
        data = []
        async for cur in _table.aggregate(pipe):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get nodes summary with {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}

@router.get('/asrank/detail')
async def get_asrank(args: ASRankQuery = Depends()):
    try:
        _table = TableSelector.get_asrank_table()
        # setting up query parameters
        query_params = dict()
        if args.asns:
            query_params['asn'] = {'$in': [int(asn) for asn in args.asns.split(',')]}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}).sort({'rank': 1}).limit(args.limit):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get asrank data with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    
@router.get('/phy-links/detail')
async def get_phy_links(args: PhyLinkQuery = Depends()):
    try:
        _table = TableSelector.get_phy_links_table()
        query_params = dict()
        if args.nidxs:
            or_expr = [{'src_nidx': {'$in': [int(idx) for idx in args.nidxs.split(',')]}}, {'dst_nidx': {'$in': [int(idx) for idx in args.nidxs.split(',')]}}]
            query_params['$or'] = or_expr
        elif args.asns:
            or_expr = [{'src_asn': {'$in': [int(asn) for asn in args.asns.split(',')]}}, {'dst_asn': {'$in': [int(asn) for asn in args.asns.split(',')]}}]
            query_params['$or'] = or_expr
        # query_params['cross_cluster'] = True
        # query_params['distance'] = {'$gt': MIN_DISTANCE, '$lt': MAX_DISTANCE}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}).limit(NB_PHY_LINK_SAMPLE):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get phy_links data with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    
@router.get('/logic-links/detail')
async def get_logic_links(args: LogicLinkQuery = Depends()):
    try:
        _table = TableSelector.get_logic_links_table()
        query_params = dict()
        if args.idxs:
            src_idx_params = {'src_node_index': {'$in': [int(idx) for idx in args.idxs.split(',')]}}
            dst_idx_params = {'dst_node_index': {'$in': [int(idx) for idx in args.idxs.split(',')]}}
            query_params['$or'] = [src_idx_params, dst_idx_params]
        if args.asns:
            src_asn_params = {'src_asn': {'$in': [int(asn) for asn in args.asns.split(',')]}}
            dst_asn_params = {'dst_asn': {'$in': [int(asn) for asn in args.asns.split(',')]}}
            query_params['$or'] = [src_asn_params, dst_asn_params]
        data = []
        pipeline = [
            {'$match': query_params},
            {'$sample': {'size': NB_LOGIC_LINK_SAMPLE}},
            {'$project': {'_id': 0}}
        ]
        async for cur in _table.aggregate(pipeline):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get logic_links data with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
    
@router.get('/logic-nodes/detail')
async def get_logic_nodes(args: LogicNodeQuery = Depends()):
    try:
        _table = TableSelector.get_logic_nodes_table()
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',') if idx.isdigit()]}
        if args.asns:
            query_params['asn'] = {'$in': [int(asn) for asn in args.asns.split(',')]}
        if args.ctys:
            query_params['country_code'] = {'$in': args.ctys.split(',')}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}).sort({'rank': 1}).limit(NB_LOGIC_NODE_SAMPLE):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get logic_nodes data with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}
