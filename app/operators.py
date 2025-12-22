# app/operators.py

from typing import List, Dict, Optional
from .models import Collection
from .operator_costs import operator_cost_excel, CostOutput
from .size_calculator import TYPE_SIZES


# ============================================================
# HELPERS
# ============================================================

def field_type_from_schema(coll: Collection, name: str) -> str:
    """
    Extract primitive type for a field from schema Collection.
    """
    for f in coll.fields:
        if f.name == name and f.field_type in TYPE_SIZES:
            return f.field_type
    raise ValueError(f"Field '{name}' not found or not primitive in collection '{coll.name}'.")


def resolve_field_types(coll: Collection, fields: List[str]) -> List[str]:
    """
    Resolve a list of field names into primitive types.
    """
    return [field_type_from_schema(coll, f) for f in fields]


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
    pk_fields: Optional[List[str]] = None
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
        doc_size=coll.doc_size
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
    pk_fields: Optional[List[str]] = None
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
    doc_size=coll.doc_size
)



# ============================================================
# NESTED LOOP JOIN — WITHOUT SHARDING
# ============================================================

def nested_loop_without_sharding(
    left: Collection,
    right: Collection,
    join_key: str,
    distinct_values: Dict[str, int],
    servers: int = 1
) -> Dict:

    ndist = distinct_values.get(join_key)
    if ndist:
        result_docs = (left.doc_count * right.doc_count) / ndist
    else:
        result_docs = 0.1 * left.doc_count * right.doc_count

    left_scan = left.doc_count * left.doc_size
    right_scan = right.doc_count * right.doc_size

    ram_volume = left_scan + right_scan + result_docs * (left.doc_size + right.doc_size)

    from .operator_costs import BANDWIDTH_Bps, RAM_Bps, CO2_RATE, PRICE_RATE, bytes_to_gb

    time_network = 0
    time_ram = ram_volume / RAM_Bps
    time_total = time_ram

    ram_gb = bytes_to_gb(ram_volume)

    return {
        "result_docs": result_docs,
        "vol_network": 0,
        "ram_volume": ram_volume,
        "time_total": time_total,
        "co2": ram_gb * CO2_RATE,
        "price": ram_gb * PRICE_RATE
    }


# ============================================================
# NESTED LOOP JOIN — WITH SHARDING
# ============================================================

def nested_loop_with_sharding(
    left: Collection,
    right: Collection,
    join_key: str,
    distinct_values: Dict[str, int],
    servers: int
) -> Dict:

    ndist = distinct_values.get(join_key)
    if ndist:
        result_docs = (left.doc_count * right.doc_count) / ndist
    else:
        result_docs = 0.1 * left.doc_count * right.doc_count

    local_left = left.doc_count / servers
    local_right = right.doc_count / servers

    ram_volume = (
        servers *
        (local_left * left.doc_size + local_right * right.doc_size)
        + result_docs * (left.doc_size + right.doc_size)
    )

    from .operator_costs import RAM_Bps, CO2_RATE, PRICE_RATE, bytes_to_gb

    time_ram = ram_volume / RAM_Bps
    ram_gb = bytes_to_gb(ram_volume)

    return {
        "result_docs": result_docs,
        "vol_network": 0,
        "ram_volume": ram_volume,
        "time_total": time_ram,
        "co2": ram_gb * CO2_RATE,
        "price": ram_gb * PRICE_RATE
    }
