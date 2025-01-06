from fastapi import APIRouter, Depends
from database.models import TableSelector
from .models import (
    NodeQuery, ASRankQuery, PhyLinkQuery, LogicLinkQuery, ClusterQuery, LogicNodeQuery
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

@router.get('/clusters/detail')
async def get_clusters(args: ClusterQuery = Depends()):
    try:
        _table = TableSelector.get_clusters_table()
        # setting up query parameters
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get clusters data with {args}, err: {e}')
        return {'data': [], 'status': 'bad', 'message': str(e)}


@router.get('/nodes/detail')
async def get_nodes(args: NodeQuery = Depends()):
    try:
        _table = TableSelector.get_nodes_table()
        # setting up query parameters
        query_params = dict()
        if args.idxs:
            query_params['index'] = {'$in': [int(idx) for idx in args.idxs.split(',')]}
        if args.asns:
            query_params['asn'] = {'$in': [int(asn) for asn in args.asns.split(',')]}
        if args.ctys:
            query_params['country'] = {'$in': args.ctys.split(',')}
        if args.cidxs:
            query_params['cluster_id'] = {'$in': [int(cidx) for cidx in args.cidxs.split(',')]}
        data = []
        async for cur in _table.find(query_params, {'_id': 0}):
            data.append(cur)
        return {'data': data, 'status': 'ok', 'message': ''}
    except Exception as e:
        logger.error(f'failed to get nodes data with {args}, err: {e}')
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
