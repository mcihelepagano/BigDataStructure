# app/cost_model/operations.py

from typing import List, Dict, Optional
from ..core.models import Collection
from .formulas import operator_cost_excel, CostOutput
from ..core.schema_tools import resolve_field_types


# ============================================================
# HELPERS
# ============================================================

def default_selectivity(filter_key: str, distinct_values: Dict[str, int]) -> float:
    """
    Default selectivity = 1 / distinct(filter_key)
    """
    if filter_key in distinct_values and distinct_values[filter_key] > 0:
        return 1.0 / distinct_values[filter_key]
    return 0.1


def detect_primary_key_result(filter_keys: List[str], pk_fields: Optional[List[str]]) -> bool:
    """
    True if filter keys include the full primary key.
    """
    if not pk_fields:
        return False
    return set(pk_fields) <= set(filter_keys)


# ============================================================
# FILTER WITHOUT SHARDING
# ============================================================

def filter_without_sharding(
    coll: Collection,
    filter_keys: List[str],
    select_fields: List[str],
    distinct_values: Dict[str, int],
    servers: int,
    selectivity: Optional[float] = None,
    pk_fields: Optional[List[str]] = None,
    servers_working: int = 1,
    indexes_per_shard: int = 1
) -> CostOutput:

    # -------------------------
    # Result cardinality
    # -------------------------
    if detect_primary_key_result(filter_keys, pk_fields):
        result_docs = 1
        sel = 1.0 / coll.doc_count
    else:
        sel = selectivity or default_selectivity(filter_keys[0], distinct_values)
        result_docs = coll.doc_count * sel

    # -------------------------
    # Field types
    # -------------------------
    query_fields = list(set(filter_keys + select_fields))
    query_types = resolve_field_types(coll, query_fields)
    result_types = resolve_field_types(coll, select_fields)
    filter_types     = resolve_field_types(coll, filter_keys)
    projection_types = resolve_field_types(coll, select_fields)

    # -------------------------
    # Local docs per server
    # -------------------------
    local_docs = coll.doc_count / servers

    # -------------------------
    # Cost computation (Excel model)
    # -------------------------
    return operator_cost_excel(
        s=servers,
        result_docs=result_docs,
        filter_types=filter_types,
        projection_types=projection_types,
        local_docs=local_docs,
        selectivity=sel,
        doc_size=coll.doc_size,
        servers_working=servers_working,
        servers_total=servers,
        indexes_per_shard=indexes_per_shard
    )


# ============================================================
# FILTER WITH SHARDING
# ============================================================

def filter_with_sharding(
    coll: Collection,
    filter_keys: List[str],
    select_fields: List[str],
    sharding_key: str,
    distinct_values: Dict[str, int],
    servers: int,
    selectivity: Optional[float] = None,
    pk_fields: Optional[List[str]] = None,
    servers_working: int = 1,
    indexes_per_shard: int = 1
) -> CostOutput:

    # -------------------------
    # Servers involved
    # -------------------------
    S = 1 if sharding_key in filter_keys else servers

    # -------------------------
    # Result cardinality
    # -------------------------
    if detect_primary_key_result(filter_keys, pk_fields):
        result_docs = 1
        sel = 1.0 / coll.doc_count
    else:
        sel = selectivity or default_selectivity(filter_keys[0], distinct_values)
        result_docs = coll.doc_count * sel

    # -------------------------
    # Field types
    # -------------------------
    filter_types     = resolve_field_types(coll, filter_keys)
    projection_types = resolve_field_types(coll, select_fields)


    # -------------------------
    # Local docs per server
    # -------------------------
    local_docs = coll.doc_count / servers

    # -------------------------
    # Cost computation (Excel model)
    # -------------------------
    return operator_cost_excel(
        s=S,
        result_docs=result_docs,
        filter_types=filter_types,
        projection_types=projection_types,
        local_docs=local_docs,
        selectivity=sel,
        doc_size=coll.doc_size,
        servers_working=S,
        servers_total=servers,  # shards involved
        indexes_per_shard=indexes_per_shard
    )



# ============================================================
# NESTED LOOP JOIN (OUTER + INNER COSTING)
# ============================================================

def nested_loop_without_sharding(
    left: Collection,
    right: Collection,
    join_key: str,
    distinct_values: Dict[str, int],
    outer_filter_keys: Optional[List[str]] = None,
    outer_select_fields: Optional[List[str]] = None,
    inner_select_fields: Optional[List[str]] = None,
    servers: int = 1
) -> Dict:
    """
    Compute nested loop costs by splitting outer and inner phases.
    - Outer: based on left collection and outer_filter_keys/outer_select_fields.
    - Inner: based on right collection, join_key as filter, and inner_select_fields.
    Total time = time_outer + time_inner * result_docs (join outputs).
    Network/RAM/CO2/price are aggregated similarly.
    """
    outer_filter_keys = outer_filter_keys or []
    outer_select_fields = outer_select_fields or []
    inner_select_fields = inner_select_fields or []

    # Selectivities
    if outer_filter_keys:
        outer_sel = default_selectivity(outer_filter_keys[0], distinct_values)
    else:
        outer_sel = 1.0

    ndist = distinct_values.get(join_key)
    inner_sel = 1.0 / ndist if ndist else 0.1

    # Outer cardinality and join result cardinality
    outer_result_docs = left.doc_count * outer_sel
    if ndist:
        join_result_docs = outer_result_docs * (right.doc_count / ndist)
    else:
        join_result_docs = 0.1 * outer_result_docs * right.doc_count

    # Outer cost (treated as a filter without sharding)
    outer_cost = operator_cost_excel(
        s=servers,
        result_docs=outer_result_docs,
        filter_types=resolve_field_types(left, outer_filter_keys) if outer_filter_keys else [],
        projection_types=resolve_field_types(left, outer_select_fields) if outer_select_fields else [],
        local_docs=left.doc_count / servers,
        selectivity=outer_sel,
        doc_size=left.doc_size,
        servers_working=servers,
        servers_total=servers,
        indexes_per_shard=1
    )

    # Inner cost per iteration (join key filter, projected inner fields)
    inner_cost = operator_cost_excel(
        s=servers,
        result_docs=1,  # per outer row
        filter_types=resolve_field_types(right, [join_key]),
        projection_types=resolve_field_types(right, inner_select_fields) if inner_select_fields else [],
        local_docs=right.doc_count / servers,
        selectivity=inner_sel,
        doc_size=right.doc_size,
        servers_working=servers,
        servers_total=servers,
        indexes_per_shard=1
    )

    total_vol_network = outer_cost.vol_network + inner_cost.vol_network * join_result_docs
    total_ram_volume = outer_cost.ram_volume_total + inner_cost.ram_volume_total * join_result_docs
    total_time = outer_cost.time_total + inner_cost.time_total * join_result_docs
    total_co2 = (
        outer_cost.co2 +
        inner_cost.co2 * join_result_docs
    )
    total_price = outer_cost.price + inner_cost.price * join_result_docs

    return {
        "result_docs": join_result_docs,
        "outer": outer_cost,
        "inner_per_iteration": inner_cost,
        "vol_network": total_vol_network,
        "ram_volume": total_ram_volume,
        "time_total": total_time,
        "co2": total_co2,
        "price": total_price
    }

def nested_loop_with_sharding(
    left: Collection,
    right: Collection,
    join_key: str,
    distinct_values: Dict[str, int],
    servers: int,
    outer_filter_keys: List[str],
    outer_select_fields: List[str],
    inner_select_fields: List[str],
    outer_sharding_key: str,
    inner_sharding_key: str,
    # NUOVO PARAMETRO: Permette di forzare la selettività (es. 0.0005 per Apple)
    outer_selectivity: Optional[float] = None 
) -> Dict:
    
    # 1. Calcolo Selettività Outer (Priorità al parametro esplicito)
    if outer_selectivity is not None:
        outer_sel = outer_selectivity
    elif outer_filter_keys:
        outer_sel = default_selectivity(outer_filter_keys[0], distinct_values)
    else:
        outer_sel = 1.0

    ndist = distinct_values.get(join_key, 1)
    inner_sel = 1.0 / ndist if ndist else 0.1

    outer_result_docs = left.doc_count * outer_sel
    join_result_docs = outer_result_docs * (right.doc_count * inner_sel)

    # Sharding Logic
    outer_match = outer_sharding_key and outer_filter_keys and outer_sharding_key in outer_filter_keys
    inner_match = inner_sharding_key and inner_sharding_key == join_key
    
    S_outer = 1 if outer_match else servers
    S_inner = 1 if inner_match else servers

    # 1. Costo OUTER
    outer_cost = operator_cost_excel(
        s=S_outer,
        result_docs=outer_result_docs,
        filter_types=resolve_field_types(left, outer_filter_keys),
        projection_types=resolve_field_types(left, outer_select_fields),
        local_docs=left.doc_count / servers,
        selectivity=outer_sel,
        doc_size=left.doc_size,
        servers_working=S_outer,
        servers_total=servers,
        indexes_per_shard=1
    )

    # 2. Costo INNER (Per SINGOLA Iterazione / Lookup)
    avg_matches_per_lookup = join_result_docs / outer_result_docs if outer_result_docs > 0 else 0
    
    inner_cost_per_lookup = operator_cost_excel(
        s=S_inner,
        result_docs=avg_matches_per_lookup, 
        filter_types=resolve_field_types(right, [join_key]),
        projection_types=resolve_field_types(right, inner_select_fields),
        local_docs=right.doc_count / servers,
        selectivity=inner_sel,
        doc_size=right.doc_size,
        servers_working=S_inner,
        servers_total=servers,
        indexes_per_shard=1
    )

    # 3. AGGREGAZIONE
    iterations = outer_result_docs 

    total_vol_network = outer_cost.vol_network + (inner_cost_per_lookup.vol_network * iterations)
    total_ram_volume = outer_cost.ram_volume_total + (inner_cost_per_lookup.ram_volume_total * iterations)
    total_time = outer_cost.time_total + (inner_cost_per_lookup.time_total * iterations)
    total_co2  = outer_cost.co2 + (inner_cost_per_lookup.co2 * iterations)
    total_price = outer_cost.price + (inner_cost_per_lookup.price * iterations)

    return {
        "result_docs": join_result_docs,
        "outer": outer_cost,
        "inner_per_iteration": inner_cost_per_lookup,
        "vol_network": total_vol_network,
        "ram_volume": total_ram_volume,
        "time_total": total_time,
        "co2": total_co2,
        "price": total_price
    }


# ============================================================
# AGGREGATE WITH SHARDING (MAP-REDUCE / SHUFFLE)
# ============================================================

def aggregate_with_sharding(
    coll: Collection,
    match_filter_keys: List[str],      # Keys for the initial Match ($match)
    group_by_key: str,                 # Key for Grouping ($group)
    project_fields: List[str],         # Fields in the output
    distinct_values: Dict[str, int],
    servers: int,
    selectivity: Optional[float] = None,
    sharding_key: str = None
) -> Dict:
    """
    Computes cost for Aggregation (Map-Reduce style):
    1. MATCH (Local): Filter docs on shards.
    2. SHUFFLE: Transfer docs to reducers (unless grouping on sharding key).
    3. REDUCE: Aggregate in RAM.
    """
    
    # --- PHASE 1: MATCH (Local Filter) ---
    # Calculates how many docs pass the filter locally
    
    # 1. Selectivity (Input -> Match)
    if match_filter_keys:
        sel = selectivity or default_selectivity(match_filter_keys[0], distinct_values)
    else:
        sel = 1.0 # No filter, full scan
        
    # Number of docs entering the pipeline
    docs_matched = coll.doc_count * sel
    
    # Servers involved in the Match phase
    # If filtering on sharding key, we only touch 1 server (Smart Routing)
    S_match = 1 if (sharding_key and match_filter_keys and sharding_key in match_filter_keys) else servers

    # Cost of Local Scan (Match)
    # Note: The output of this phase is NOT sent to client, but to the Shuffle phase.
    # So 'result_docs' here is just for RAM calculation, not network (yet).
    cost_match = operator_cost_excel(
        s=S_match,
        result_docs=docs_matched, 
        filter_types=resolve_field_types(coll, match_filter_keys),
        projection_types=[], # No projection sent to user yet
        local_docs=coll.doc_count / servers,
        selectivity=sel,
        doc_size=coll.doc_size,
        servers_working=S_match,
        servers_total=servers
    )
    
    # --- PHASE 2: SHUFFLE (Network Transfer) ---
    # If GroupBy Key == Sharding Key, aggregation is local (No Shuffle).
    # Else, we must send (GroupKey, Doc) to the correct reducer.
    
    is_local_aggregation = (sharding_key and group_by_key == sharding_key)
    
    if is_local_aggregation:
        vol_shuffle = 0
        time_shuffle = 0
        co2_shuffle = 0
    else:
        # We send the 'group_by_key' + any fields needed for aggregation/projection
        # For simplicity, let's assume we send the projected fields + group key
        shuffle_fields = list(set(project_fields + [group_by_key]))
        size_shuffle_msg = 12 + sum(TYPE_SIZES.get(t, 80) for t in resolve_field_types(coll, shuffle_fields))
        
        # Volume = Docs_Matched * Message_Size
        vol_shuffle = docs_matched * size_shuffle_msg
        
        # Time & Cost of Shuffle
        from ..config import BANDWIDTH_Bps, CO2_NETWORK_RATE, PRICE_RATE
        time_shuffle = vol_shuffle / BANDWIDTH_Bps
        
        # Shuffle Cost (Network only)
        # Note: Price usually pays for "Data Transfer Out". 
        # Internal cluster traffic might be cheaper, but in this model network is network.
        shuffle_gb = vol_shuffle / 1_000_000_000.0
        co2_shuffle = shuffle_gb * CO2_NETWORK_RATE
        price_shuffle = shuffle_gb * PRICE_RATE # Often internal traffic is free, check if needed. Assuming paid.

    # --- PHASE 3: REDUCE (Output) ---
    # The result is grouped. Number of output docs = Distinct Values of Group Key.
    # But usually limited by LIMIT X (e.g., Top 100).
    # Let's assume the result_docs is the Distinct Count of Group Key (worst case) or 1 if singular.
    
    ndist_group = distinct_values.get(group_by_key, 100) # Default match Excel behavior
    # If we have a filter, the number of groups might be reduced. 
    # Logic: groups_out = min(docs_matched, distinct_groups)
    result_docs = min(docs_matched, ndist_group)
    
    # Cost of sending Final Result to Client (from Reducer)
    # Usually 1 server (the one doing the final merge) sends the result
    cost_reduce_out = operator_cost_excel(
        s=1, # Final merge usually on 1 coordinator
        result_docs=result_docs,
        filter_types=[], 
        projection_types=resolve_field_types(coll, project_fields),
        local_docs=0, # No scan, just RAM output
        selectivity=0,
        doc_size=0,
        servers_working=1,
        servers_total=servers
    )
    
    # --- TOTALS ---
    total_vol_network = cost_match.vol_network + vol_shuffle + cost_reduce_out.vol_network
    
    # RAM: Match Scan + Reduce RAM (Holding counters in memory)
    # Reduce RAM ~ Groups * Size_Accumulator. Typically small compared to Scan.
    total_ram_volume = cost_match.ram_volume_total 
    
    total_time = cost_match.time_total + time_shuffle + cost_reduce_out.time_total
    
    total_co2 = cost_match.co2 + co2_shuffle + cost_reduce_out.co2
    total_price = cost_match.price + (price_shuffle if not is_local_aggregation else 0) + cost_reduce_out.price

    return {
        "result_docs": result_docs,
        "vol_network": total_vol_network,
        "vol_shuffle": vol_shuffle,  # Useful debug metric mentioned in PDF
        "ram_volume": total_ram_volume,
        "time_total": total_time,
        "co2": total_co2,
        "price": total_price
    }