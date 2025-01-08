import traceback
import logging


logger = logging.getLogger('database.services')


def _bulk_write(_table, _ops):
    if not _ops:
        return

    _ops = [_op for _op in _ops if _op is not None]
    nb_inserted = len(_ops)
    if nb_inserted == 0:
        return 0
    try:
        _table.insert_many(_ops)
    except Exception as e:
        logger.error(f"Failed to bulk write for {_table} with len={len(_ops)},"
                     f" err: {e}, stack: {traceback.format_exc()}")
        return 0
    return nb_inserted
