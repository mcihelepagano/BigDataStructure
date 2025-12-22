# app/operator_costs.py

from dataclasses import dataclass
from typing import List
from .size_calculator import TYPE_SIZES, OVERHEAD


# ================================
# CONSTANTS (MATCHING EXCEL)
# ================================

BANDWIDTH_Bps = 100 * 1024 * 1024        # 100 MB/s
RAM_Bps       = 25 * 1024 * 1024 * 1024  # 25 GB/s

# Environmental impact factors (per GB)
CO2_NETWORK_RATE = 0.0110  # kg CO2-eq / GB (bandwidth)
CO2_RAM_RATE     = 0.0280  # kg CO2-eq / GB (RAM/CPU)

PRICE_RATE = 0.011   # ? per GB

INDEX_SIZE = 1_000_000   # 1 MB, as seen in Excel "local index"


# ================================
# OUTPUT OBJECT
# ================================

@dataclass
class CostOutput:
    size_query: float
    size_msg: float
    vol_network: float

    ram_volume: float
    time_network: float
    time_ram: float
    time_total: float

    co2: float
    price: float


# ================================
# HELPER FUNCTIONS
# ================================

def compute_field_size(field_type: str) -> int:
    """Return only VALUE size based on primitive type."""
    return TYPE_SIZES[field_type]


def compute_attribute_size(field_type: str) -> int:
    """12-byte key + primitive value size."""
    return OVERHEAD + compute_field_size(field_type)


def size_of_fields(field_types: List[str]) -> int:
    """S (12 + size(type))"""
    return sum(compute_attribute_size(t) for t in field_types)


def bytes_to_gb(x: float) -> float:
    return x / (1024 ** 3)


def compute_query_size(
    filter_types: List[str],
    projection_types: List[str]
) -> int:
    """
    Query size model (user-defined):

    QS =
      sum(value sizes of filters)
    + sum(value sizes of projections)
    + (#filters + #projections) * 12
    """
    value_filters = sum(TYPE_SIZES[t] for t in filter_types)
    value_proj    = sum(TYPE_SIZES[t] for t in projection_types)
    key_overhead  = (len(filter_types) + len(projection_types)) * OVERHEAD

    return value_filters + value_proj + key_overhead



# ================================================================
# CORE: COST MODEL EXACTLY AS IN EXCEL (NETWORK + RAM)
# ================================================================

def operator_cost_excel(
    s: int,                     # number of servers contacted
    result_docs: int,           # number of matching docs
    filter_types: List[str],
    projection_types: List[str],
    local_docs: int,            # docs on each server (collection.doc_count / servers)
    selectivity: float,
    doc_size: int               # document size (HW2)
) -> CostOutput:

    # -------------------------
    # 1. QUERY + OUTPUT SIZES
    # -------------------------
    size_query = compute_query_size(
        filter_types=filter_types,
        projection_types=projection_types
    )

    size_msg   = size_of_fields(projection_types)

    # -------------------------
    # 2. NETWORK VOLUME (Excel)
    #    NetworkVol = SⅧS + resⅥS
    # -------------------------
    vol_network = s * size_query + result_docs * size_msg

    # -------------------------
    # 3. LOCAL RAM ACCESSED PER SERVER
    #
    # Excel formula:
    # RAM_local = max( INDEX_SIZE , local_docs * selectivity * doc_size )
    # -------------------------
    ram_local_scan = local_docs * selectivity * doc_size
    ram_local = max(INDEX_SIZE, ram_local_scan)

    # -------------------------
    # 4. RAM OUTPUT COST
    #
    # Excel term: K * output_size
    # -------------------------
    ram_output = result_docs * size_msg

    # -------------------------
    # 5. TOTAL RAM
    #
    # RAM_total = ram_output + ram_local * nb_servers_working
    #
    # nb_servers_working = s (same as network)
    # -------------------------
    ram_volume = ram_output + ram_local * s

    # -------------------------
    # 6. TIME
    # -------------------------
    time_network = vol_network / BANDWIDTH_Bps
    time_ram     = ram_volume   / RAM_Bps
    time_total   = time_network + time_ram

    # -------------------------
    # 7. CO2 & PRICE
    #
    # CO2_network = vol_network * CO2_NETWORK_RATE
    # CO2_RAM     = ram_volume * CO2_RAM_RATE
    # total CO2   = CO2_network + CO2_RAM
    # -------------------------
    ram_gb = bytes_to_gb(ram_volume)
    net_gb = bytes_to_gb(vol_network)

    co2_network = net_gb * CO2_NETWORK_RATE
    co2_ram     = ram_gb * CO2_RAM_RATE
    co2         = co2_network + co2_ram

    price  = ram_gb * PRICE_RATE

    return CostOutput(
        size_query=size_query,
        size_msg=size_msg,
        vol_network=vol_network,
        ram_volume=ram_volume,
        time_network=time_network,
        time_ram=time_ram,
        time_total=time_total,
        co2=co2,
        price=price
    )
