# app/core/schema_tools.py

from typing import List, Dict, Optional
from .models import Collection, Field
from .size_calc import TYPE_SIZES


# ============================================================
# HELPERS
# ============================================================

def _find_field(fields, name: str) -> Optional[Field]:
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


def _collect_primitive_types(field: Field) -> List[str]:
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