# etl_settings.py
import json, os

ETL_GEAR = {
    'num_workers': 6,
    'batch_size': 10000,
    'oracle_prefetch': 10000,
    'log_interval': 10000,
}

_OVERRIDE = os.path.join(os.path.dirname(__file__), 'etl_settings_override.json')
try:
    with open(_OVERRIDE) as _f:
        _overrides = json.load(_f)
    ETL_GEAR['num_workers'] = int(_overrides.get('num_workers', ETL_GEAR['num_workers']))
    ETL_GEAR['batch_size']  = int(_overrides.get('batch_size',  ETL_GEAR['batch_size']))
except (FileNotFoundError, ValueError, KeyError):
    pass
