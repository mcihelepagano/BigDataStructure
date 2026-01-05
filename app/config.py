# app/config.py

BANDWIDTH_Bps = 100_000_000        # 100 MB/s
RAM_Bps       = 25_000_000_000     # 25 GB/s

# Environmental impact factors (per GB)
CO2_NETWORK_RATE = 0.0110  # kg CO2-eq / GB (bandwidth)
CO2_RAM_RATE     = 0.0280  # kg CO2-eq / GB (RAM/CPU)

PRICE_RATE = 0.011   # EUR per GB (network volume)

INDEX_SIZE = 1_000_000   # 1 MB, as seen in Excel "local index"
JSON_KEY_OVERHEAD = 12