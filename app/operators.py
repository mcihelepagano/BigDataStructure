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
    outer_filter_keys: Optional[List[str]] = None,
    outer_select_fields: Optional[List[str]] = None,
    inner_select_fields: Optional[List[str]] = None,
    sharding_key: Optional[str] = None
) -> Dict:
    """
    Sharded nested loop: same outer/inner cost splitting, but assume perfect sharding on join_key
    so s=1 for both outer and inner.
    """
    outer_filter_keys = outer_filter_keys or []
    outer_select_fields = outer_select_fields or []
    inner_select_fields = inner_select_fields or []

    if outer_filter_keys:
        outer_sel = default_selectivity(outer_filter_keys[0], distinct_values)
    else:
        outer_sel = 1.0

    ndist = distinct_values.get(join_key)
    inner_sel = 1.0 / ndist if ndist else 0.1

    outer_result_docs = left.doc_count * outer_sel
    if ndist:
        join_result_docs = outer_result_docs * (right.doc_count / ndist)
    else:
        join_result_docs = 0.1 * outer_result_docs * right.doc_count

    # servers touched based on sharding key (like filter_with_sharding)
    sharding_matches_outer = sharding_key and outer_filter_keys and sharding_key in outer_filter_keys
    sharding_matches_join  = sharding_key and sharding_key == join_key
    S = 1 if (sharding_matches_outer or sharding_matches_join) else servers

    outer_cost = operator_cost_excel(
        s=S,
        result_docs=outer_result_docs,
        filter_types=resolve_field_types(left, outer_filter_keys) if outer_filter_keys else [],
        projection_types=resolve_field_types(left, outer_select_fields) if outer_select_fields else [],
        local_docs=left.doc_count / servers,
        selectivity=outer_sel,
        doc_size=left.doc_size,
        servers_working=S,
        servers_total=S,
        indexes_per_shard=1
    )

    inner_cost = operator_cost_excel(
        s=S,
        result_docs=1,
        filter_types=resolve_field_types(right, [join_key]),
        projection_types=resolve_field_types(right, inner_select_fields) if inner_select_fields else [],
        local_docs=right.doc_count / servers,
        selectivity=inner_sel,
        doc_size=right.doc_size,
        servers_working=S,
        servers_total=S,
        indexes_per_shard=1
    )

    total_vol_network = outer_cost.vol_network + inner_cost.vol_network * join_result_docs
    total_ram_volume = outer_cost.ram_volume_total + inner_cost.ram_volume_total * join_result_docs
    total_time = outer_cost.time_total + inner_cost.time_total * join_result_docs
    total_co2 = outer_cost.co2 + inner_cost.co2 * join_result_docs
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
