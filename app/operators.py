# app/operators.py

from typing import List, Dict, Optional
from .operator_costs import filter_cost_professor, CostOutput
from .models import Collection
from .size_calculator import TYPE_SIZES, OVERHEAD


# ============================================================
# HELPERS
# ============================================================

def field_type_from_schema(coll: Collection, name: str) -> str:
    """
    Extract primitive type for field from schema Collection.
    Raise error if field is missing or not primitive.
    """
    for f in coll.fields:
        if f.name == name and f.field_type in TYPE_SIZES:
            return f.field_type
    raise ValueError(f"Field '{name}' not found or not primitive in collection '{coll.name}'.")


def resolve_field_types(coll: Collection, fields: List[str]) -> List[str]:
    """
    Given a list of SELECT/WHERE field names, return their primitive types.
    """
    return [field_type_from_schema(coll, name) for name in fields]


def default_selectivity(filter_key: str, distinct_values: Dict[str, int]) -> float:
    """
    Default selectivity = 1 / Ndistinct(filter_key).
    """
    if filter_key in distinct_values and distinct_values[filter_key] > 0:
        return 1.0 / distinct_values[filter_key]
    return 0.1


def detect_primary_key_result(filter_keys: List[str], pk_fields: Optional[List[str]]) -> bool:
    """
    Returns True if the filter includes the entire PK.
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
    pk_fields: Optional[List[str]] = None
) -> CostOutput:

    # Determine result cardinality
    if detect_primary_key_result(filter_keys, pk_fields):
        result_docs = 1
    else:
        if selectivity is None:
            selectivity = default_selectivity(filter_keys[0], distinct_values)
        result_docs = coll.doc_count * selectivity

    # Determine types of involved fields
    query_fields = list(set(filter_keys + select_fields))
    query_types = resolve_field_types(coll, query_fields)
    result_types = resolve_field_types(coll, select_fields)

    # Compute cost (S = servers)
    c = filter_cost_professor(
        s=servers,
        result_docs=result_docs,
        query_field_types=query_types,
        result_field_types=result_types,
    )

    # Store output size
    c.total_result_size = result_docs * c.size_msg
    return c


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
    pk_fields: Optional[List[str]] = None
) -> CostOutput:

    # Determine number of servers involved
    S = 1 if sharding_key in filter_keys else servers

    # Determine result cardinality
    if detect_primary_key_result(filter_keys, pk_fields):
        result_docs = 1
    else:
        if selectivity is None:
            selectivity = default_selectivity(filter_keys[0], distinct_values)
        result_docs = coll.doc_count * selectivity

    # Resolve types
    query_fields = list(set(filter_keys + select_fields))
    query_types = resolve_field_types(coll, query_fields)
    result_types = resolve_field_types(coll, select_fields)

    # Compute cost
    c = filter_cost_professor(
        s=S,
        result_docs=result_docs,
        query_field_types=query_types,
        result_field_types=result_types,
    )

    c.total_result_size = result_docs * c.size_msg
    return c


# ============================================================
# NESTED LOOP WITHOUT SHARDING
# ============================================================

def nested_loop_without_sharding(
    left: Collection,
    right: Collection,
    join_key: str,
    distinct_values: Dict[str, int]
) -> Dict:
    """
    Generic nested loop join with no sharding.
    """
    # Cardinality estimate
    ndist = distinct_values.get(join_key, None)
    if ndist and ndist > 0:
        result_docs = (left.doc_count * right.doc_count) / ndist
    else:
        result_docs = 0.1 * left.doc_count * right.doc_count

    # Document sizes
    left_doc_size = sum(OVERHEAD + TYPE_SIZES[f.field_type] for f in left.fields if f.field_type in TYPE_SIZES)
    right_doc_size = sum(OVERHEAD + TYPE_SIZES[f.field_type] for f in right.fields if f.field_type in TYPE_SIZES)

    size_msg = left_doc_size + right_doc_size

    # Smaller relation moved in network
    left_bytes = left.doc_count * left_doc_size
    right_bytes = right.doc_count * right_doc_size
    smaller = min(left_bytes, right_bytes)

    vol_network = smaller + result_docs * size_msg

    # Compute time/cost parameters like filter
    from .operator_costs import BANDWIDTH_Bps, RAM_Bps, CO2_RATE, PRICE_RATE, bytes_to_gb
    time_network = vol_network / BANDWIDTH_Bps
    time_cpu = vol_network / RAM_Bps
    time_total = time_network + time_cpu

    vol_gb = bytes_to_gb(vol_network)
    co2 = vol_gb * CO2_RATE
    price = vol_gb * PRICE_RATE

    return {
        "result_docs": result_docs,
        "size_msg": size_msg,
        "vol_network": vol_network,
        "time_total": time_total,
        "co2": co2,
        "price": price
    }


# ============================================================
# NESTED LOOP WITH SHARDING
# ============================================================

def nested_loop_with_sharding(
    left: Collection,
    right: Collection,
    join_key: str,
    distinct_values: Dict[str, int],
    servers: int
) -> Dict:
    """
    Join with perfect sharding alignment: no data movement except result.
    """

    # Cardinality
    ndist = distinct_values.get(join_key, None)
    if ndist:
        result_docs = (left.doc_count * right.doc_count) / ndist
    else:
        result_docs = 0.1 * left.doc_count * right.doc_count

    # Document sizes
    left_doc_size = sum(OVERHEAD + TYPE_SIZES[f.field_type] for f in left.fields if f.field_type in TYPE_SIZES)
    right_doc_size = sum(OVERHEAD + TYPE_SIZES[f.field_type] for f in right.fields if f.field_type in TYPE_SIZES)

    size_msg = left_doc_size + right_doc_size
    vol_network = result_docs * size_msg

    # Compute cost
    from .operator_costs import BANDWIDTH_Bps, RAM_Bps, CO2_RATE, PRICE_RATE, bytes_to_gb
    time_network = vol_network / BANDWIDTH_Bps
    time_cpu = vol_network / RAM_Bps
    time_total = time_network + time_cpu

    vol_gb = bytes_to_gb(vol_network)
    co2 = vol_gb * CO2_RATE
    price = vol_gb * PRICE_RATE

    return {
        "result_docs": result_docs,
        "size_msg": size_msg,
        "vol_network": vol_network,
        "time_total": time_total,
        "co2": co2,
        "price": price
    }
