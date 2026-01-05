# app/cost_model/formulas.py

from dataclasses import dataclass
from typing import List
from ..core.size_calc import TYPE_SIZES, OVERHEAD
from ..config import BANDWIDTH_Bps, RAM_Bps, CO2_NETWORK_RATE, CO2_RAM_RATE, PRICE_RATE, INDEX_SIZE


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
    return x / 1_000_000_000.0


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
    
    # Costo per i server che lavorano (Indice + Dati)
    active_ram_cost = servers_working * ram_local
    
    # Costo per i server inattivi
    # NUOVA LOGICA: Se s (server contattati) è 1, assumiamo accesso diretto (Smart Routing).
    # I server inattivi NON vengono toccati/allocati per questa operazione.
    # Se s > 1 (o se fosse una Join distribuita che riserva risorse), allora pagano l'indice.
    
    inactive_count = max(servers_total - servers_working, 0)
    
    if s == 1 and servers_total > 1:
        # Caso Q1: Point Query su Sharding Key. 
        # Routing intelligente: non paghiamo per i server inattivi.
        inactive_ram_cost = 0 
    else:
        # Caso Standard / Scatter-Gather:
        # Assumiamo che le risorse siano riservate o che la query tocchi potenzialmente il cluster.
        # (Oppure mantieni la logica precedente se vuoi essere conservativo, 
        # ma per Q1 Excel dice 0).
        inactive_ram_cost = inactive_count * (indexes_per_shard * INDEX_SIZE)
    
    ram_volume_total = active_ram_cost + inactive_ram_cost

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
