# app/operator_costs.py

from dataclasses import dataclass
from typing import List
from .size_calculator import TYPE_SIZES, OVERHEAD


# ================================
# FIXED CONSTANTS (Professor Model)
# ================================

# Network bandwidth: 100 MB/s
BANDWIDTH_Bps = 100 * 1024 * 1024       # Bytes per second

# RAM throughput: 25 GB/s
RAM_Bps = 25 * 1024 * 1024 * 1024       # Bytes per second

# Carbon footprint and price per GB transferred
CO2_RATE   = 0.011   # kg CO₂ per GB
PRICE_RATE = 0.011   # EUR per GB


# ================================
# OUTPUT OBJECT
# ================================

@dataclass
class CostOutput:
    size_query: float
    size_msg: float
    vol_network: float
    time_network: float
    time_cpu: float
    time_total: float
    co2: float
    price: float


# ================================
# HELPER FUNCTIONS
# ================================

def compute_field_size(field_type: str) -> int:
    """
    Returns VALUE size of a primitive field based on TYPE_SIZES.
    Example:
      integer -> 8
      string -> 80
    """
    return TYPE_SIZES[field_type]


def compute_attribute_size(field_type: str) -> int:
    """
    OVERHEAD (12 bytes) + VALUE_SIZE(type)
    This reproduces exactly the professor's model.
    """
    return OVERHEAD + compute_field_size(field_type)


def size_of_fields(field_types: List[str]) -> int:
    """
    Given a list of primitive field types:
      ["integer", "string", "date"]
    compute total size = Σ (12 + type_size)
    """
    total = 0
    for t in field_types:
        total += compute_attribute_size(t)
    return total


def bytes_to_gb(b: float) -> float:
    return b / (1024 ** 3)


# ================================
# CORE FILTER COST MODEL
# ================================

def filter_cost_professor(
    s: int,                         # number of servers contacted
    result_docs: int,               # number of output documents
    query_field_types: List[str],   # SELECT + WHERE types
    result_field_types: List[str],  # output doc types
) -> CostOutput:

    # -------------------------
    # size_query
    # -------------------------
    size_query = size_of_fields(query_field_types)

    # -------------------------
    # size_msg
    # -------------------------
    size_msg = size_of_fields(result_field_types)

    # -------------------------
    # vol_network = S·size_query + res_q·size_msg
    # -------------------------
    vol_network = s * size_query + result_docs * size_msg

    # -------------------------
    # TIMES (network + CPU)
    # -------------------------
    time_network = vol_network / BANDWIDTH_Bps
    time_cpu     = vol_network / RAM_Bps
    time_total   = time_network + time_cpu

    # -------------------------
    # ENVIRONMENTAL + PRICE IMPACT
    # -------------------------
    vol_gb = bytes_to_gb(vol_network)
    co2    = vol_gb * CO2_RATE
    price  = vol_gb * PRICE_RATE

    return CostOutput(
        size_query=size_query,
        size_msg=size_msg,
        vol_network=vol_network,
        time_network=time_network,
        time_cpu=time_cpu,
        time_total=time_total,
        co2=co2,
        price=price
    )
