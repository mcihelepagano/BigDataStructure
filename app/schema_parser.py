import json
from typing import Dict, List
from .models import Field, Collection

# schema file is json file
# doc_counts: a dict like {"Product": 100000, "Stock": 20000000, ...}which tells how many documents each collection has

"""
@param schema_file: path to the JSON Schema file
@param doc_counts: dictionary with document counts per collection
@param array_hints: optional mapping of field paths to average array lengths (e.g. {"Product.categories": 2})
@return: dictionary of Collection objects e.g {"Product": Collection(...), "Stock": Collection(...), ...}
"""


def _parse_properties(props: Dict, path: str, array_hints: Dict[str, int]) -> List[Field]:
    """
    Recursively parse JSON schema properties into Field objects.
    path: dot-separated path used to resolve array hints (e.g. Product.categories)
    """
    fields: List[Field] = []
    for fname, fdef in props.items():
        ftype = fdef.get("type", "string").lower()  # default to string if type not specified
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

            # resolve average items from hints or inline "avg_items"
            avg_items = fdef.get("avg_items") or array_hints.get(full_path) or 1

            if item_type == "object":
                subfields = _parse_properties(items.get("properties", {}), f"{full_path}[]", array_hints)
                item_field = Field(name=f"{fname}_item", field_type="object", subfields=subfields)
            else:
                item_field = Field(name=f"{fname}_item", field_type=item_type)

            fields.append(Field(name=fname, field_type="array", subfields=[item_field], avg_items=avg_items))
        else:
            fields.append(Field(name=fname, field_type=ftype))
    return fields


def parse_schema(schema_file: str, doc_counts: dict, array_hints: dict = None) -> dict:
    with open(schema_file) as f:
        schema = json.load(f)
    collections = {}
    array_hints = array_hints or {}

    for coll_name, coll_def in schema["properties"].items():
        props = coll_def.get("properties", {})
        fields = _parse_properties(props, coll_name, array_hints)
        collections[coll_name] = Collection(
            name=coll_name,
            fields=fields,
            doc_count=doc_counts.get(coll_name, 0)
        )

    print("=== Schema Parsing Results ===")
    print(f"Parsed {len(collections)} collections from schema.")
    print(", ".join(collections.keys()))  

    return collections
