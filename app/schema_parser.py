import json
from .models import Field, Collection

def parse_schema(schema_file: str, doc_counts: dict) -> dict:
    """Parse a JSON Schema and return a dictionary of Collection objects."""
    with open(schema_file) as f:
        schema = json.load(f)
    collections = {}
    for coll_name, coll_def in schema["properties"].items():
        fields = []
        props = coll_def.get("items", {}).get("properties", {})
        for fname, fdef in props.items():
            ftype = fdef.get("type", "string").lower()
            fields.append(Field(name=fname, field_type=ftype))
        collections[coll_name] = Collection(name=coll_name, fields=fields, doc_count=doc_counts.get(coll_name, 0))
    return collections
