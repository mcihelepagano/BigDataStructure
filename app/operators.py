# app/operators.py

from typing import List, Dict, Optional
from .models import Collection
from .operator_costs import operator_cost_excel, CostOutput
from .size_calculator import TYPE_SIZES


# ============================================================
# HELPERS
# ============================================================

def _find_field(fields, name: str) -> Optional['Field']:
    """
    Recursively search for a field by name (could be object/array/primitive).
    """
    for f in fields:
        if f.name == name:
            return f
        if f.field_type == "object":
            nested = _find_field(f.subfields, name)
            if nested:
                return nested
        if f.field_type == "array" and f.subfields:
            nested = _find_field(f.subfields, name)
            if nested:
                return nested
    return None


def _collect_primitive_types(field: 'Field') -> List[str]:
    """
    Return all primitive types contained in the given field.
    - primitive: singleton list
    - object: recurse into subfields
    - array: recurse into item definition
    """
    if field.field_type in TYPE_SIZES:
        return [field.field_type]
    if field.field_type == "object":
        types: List[str] = []
        for sub in field.subfields:
            types.extend(_collect_primitive_types(sub))
        return types
    if field.field_type == "array" and field.subfields:
        return _collect_primitive_types(field.subfields[0])
    raise ValueError(f"Field '{field.name}' is unsupported for type resolution.")


def field_type_from_schema(coll: Collection, name: str) -> str:
    """
    Extract primitive type for a field from schema Collection (supports nested objects/arrays).
    If the name refers to an object/array, returns the first primitive type found inside.
    """
    fld = _find_field(coll.fields, name)
    if not fld:
        raise ValueError(f"Field '{name}' not found in collection '{coll.name}'.")
    prims = _collect_primitive_types(fld)
    if prims:
        return prims[0]
    raise ValueError(f"Field '{name}' not primitive in collection '{coll.name}'.")


def resolve_field_types(coll: Collection, fields: List[str]) -> List[str]:
    """
    Resolve a list of field names into primitive types.
    If a name points to an object/array, all contained primitive types are included.
    """
    types: List[str] = []
    for fname in fields:
        fld = _find_field(coll.fields, fname)
        if not fld:
            raise ValueError(f"Field '{fname}' not found in collection '{coll.name}'.")
        types.extend(_collect_primitive_types(fld))
    return types


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
        servers_working=servers_working,
        servers_total=S,  # shards involved
        indexes_per_shard=indexes_per_shard
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

    from .operator_costs import RAM_Bps, CO2_RAM_RATE, PRICE_RATE, bytes_to_gb

    time_network = 0
    time_ram = ram_volume / RAM_Bps
    time_total = time_ram

    ram_gb = bytes_to_gb(ram_volume)

    return {
        "result_docs": result_docs,
        "vol_network": 0,
        "ram_volume": ram_volume,
        "time_total": time_total,
        "co2": ram_gb * CO2_RAM_RATE,
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

    from .operator_costs import RAM_Bps, CO2_RAM_RATE, PRICE_RATE, bytes_to_gb

    time_ram = ram_volume / RAM_Bps
    ram_gb = bytes_to_gb(ram_volume)

    return {
        "result_docs": result_docs,
        "vol_network": 0,
        "ram_volume": ram_volume,
        "time_total": time_ram,
        "co2": ram_gb * CO2_RAM_RATE,
        "price": ram_gb * PRICE_RATE
    }
