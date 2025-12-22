# app/schema_parser.py

import json
from typing import Dict, List
from .models import Field, Collection, Database
from .size_calculator import doc_size


def _parse_properties(props: Dict, path: str, array_hints: Dict[str, int]) -> List[Field]:
    fields: List[Field] = []

    for fname, fdef in props.items():
        ftype = fdef.get("type", "string").lower()
        full_path = f"{path}.{fname}" if path else fname

        fmt = fdef.get("format")
        if fmt == "date":
            ftype = "date"
        elif fmt == "longstring":
            ftype = "longstring"

        if ftype == "object":
            sub_props = fdef.get("properties", {})
            subfields = _parse_properties(sub_props, full_path, array_hints)
            fields.append(Field(name=fname, field_type="object", subfields=subfields))

        elif ftype == "array":
            items = fdef.get("items", {})
            item_type = items.get("type", "string").lower()
            avg_items = fdef.get("avg_items") or array_hints.get(full_path) or 1

            if item_type == "object":
                subfields = _parse_properties(items.get("properties", {}), full_path + "[]", array_hints)
                item_field = Field(name=f"{fname}_item", field_type="object", subfields=subfields)
            else:
                item_field = Field(name=f"{fname}_item", field_type=item_type)

            fields.append(Field(
                name=fname,
                field_type="array",
                subfields=[item_field],
                avg_items=avg_items
            ))

        else:
            fields.append(Field(name=fname, field_type=ftype))

    return fields


def parse_schema(schema_file: str, doc_counts: dict, array_hints: dict = None) -> Database:
    with open(schema_file) as f:
        schema = json.load(f)

    array_hints = array_hints or {}
    collections = {}

    for coll_name, coll_def in schema["properties"].items():
        props = coll_def.get("properties", {})
        fields = _parse_properties(props, coll_name, array_hints)

        coll = Collection(
            name=coll_name,
            fields=fields,
            doc_count=doc_counts.get(coll_name, 0)
        )

        # 🔑 CRITICAL ADDITION (HW2 → HW3 bridge)
        coll.doc_size = doc_size(coll)

        collections[coll_name] = coll

    db_name = schema.get("title", "Database")
    return Database(name=db_name, collections=collections)
