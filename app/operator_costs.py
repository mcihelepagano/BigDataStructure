# app/operator_costs.py

from dataclasses import dataclass
from typing import List
from .size_calculator import TYPE_SIZES, OVERHEAD


# ================================
# CONSTANTS (MATCHING EXCEL)
# ================================

BANDWIDTH_Bps = 100_000_000      # 100 MB/s (1e8)
RAM_Bps       = 25_000_000_000   # 25 GB/s (2.5e10)

# Environmental impact factors (per GB)
CO2_NETWORK_RATE = 0.0110  # kg CO2-eq / GB (bandwidth)
CO2_RAM_RATE     = 0.0280  # kg CO2-eq / GB (RAM/CPU)

PRICE_RATE = 0.011   # EUR per GB (network volume)

INDEX_SIZE = 1_000_000   # 1 MB, as seen in Excel "local index"


# ================================
# OUTPUT OBJECT
# ================================

@dataclass
class CostOutput:
    result_docs: float           # number of output documents
    result_size_bytes: float     # total size of output (result_docs * size_msg)

    size_query: float
    size_msg: float
    vol_network: float

    ram_volume: float             # per working shard (ram_local + ram_output split as before)
    ram_volume_total: float       # total RAM with active + inactive shards
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
    s: int,                 # number of servers contacted
    result_docs: int,       # number of matching docs
    filter_types: List[str],
    projection_types: List[str],
    local_docs: int,        # docs on each server
    selectivity: float,
    doc_size: int,          # document size
    servers_working: int = 1,
    servers_total: int = 1,
    indexes_per_shard: int = 1
) -> CostOutput:

    # -------------------------
    # 1. QUERY + OUTPUT SIZES
    # -------------------------
    size_query = compute_query_size(filter_types, projection_types)
    size_msg   = size_of_fields(projection_types)

    # -------------------------
    # 2. NETWORK VOLUME
    # -------------------------
    vol_network = s * size_query + result_docs * size_msg

    # -------------------------
    # 3. LOCAL RAM ACCESSED (PER WORKING SERVER)
    # -------------------------
    # Formula Excel: Indice + (Docs Letti * Dimensione Doc)
    # CORREZIONE: Si usa la SOMMA (+), non MAX().
    # CORREZIONE: La RAM di output (scrittura) NON viene contata qui.
    
    ram_scanned_data = local_docs * selectivity * doc_size
    ram_local = (indexes_per_shard * INDEX_SIZE) + ram_scanned_data

    # -------------------------
    # 4. TOTAL RAM (COST MODEL)
    # -------------------------
    # Formula Excel: =IF(Sharding, Active*FullRAM + Inactive*IndexRAM)
    # CORREZIONE: I server inattivi pagano solo l'indice.
    
    # Costo per i server che lavorano (Indice + Dati)
    active_ram_cost = servers_working * ram_local
    
    # Costo per i server che non lavorano (Solo Indice)
    inactive_count = max(servers_total - servers_working, 0)
    inactive_ram_cost = inactive_count * (indexes_per_shard * INDEX_SIZE)
    
    ram_volume_total = active_ram_cost + inactive_ram_cost

    # Nota: ram_volume (per il calcolo del tempo singolo server) è ram_local
    ram_volume = ram_local

    # -------------------------
    # 5. TIME (ZERO-COPY LOGIC)
    # -------------------------
    time_network = vol_network / BANDWIDTH_Bps
    
    # CORREZIONE: Il tempo RAM si basa solo su ciò che viene LETTO (ram_local).
    # La scrittura dell'output è considerata zero-copy/streaming verso la rete.
    time_ram     = ram_local / RAM_Bps 
    
    time_total   = time_network + time_ram

    # -------------------------
    # 6. CO2 & PRICE
    # -------------------------
    # Usiamo le funzioni helper per convertire in GB decimali (o binari se preferisci mantenere coerenza interna,
    # ma Excel usa conversioni dirette sui volumi calcolati).
    
    ram_gb = bytes_to_gb(ram_volume_total)
    net_gb = bytes_to_gb(vol_network)

    co2_network = net_gb * CO2_NETWORK_RATE
    co2_ram     = ram_gb * CO2_RAM_RATE
    co2         = co2_network + co2_ram

    price = net_gb * PRICE_RATE

    return CostOutput(
        result_docs=result_docs,
        result_size_bytes=result_docs * size_msg,
        size_query=size_query,
        size_msg=size_msg,
        vol_network=vol_network,
        ram_volume=ram_volume,
        ram_volume_total=ram_volume_total,
        time_network=time_network,
        time_ram=time_ram,
        time_total=time_total,
        co2=co2,
        price=price
    )
