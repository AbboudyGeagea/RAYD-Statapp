# etl_settings.py

ETL_GEAR = {
    "num_workers": 4,          # Threads for parallel upsert
    "batch_size": 5000,        # Rows per chunk
    "oracle_prefetch": 10000,   # Oracle fetch buffer
    "log_interval": 10000      # Progress logging frequency
}
