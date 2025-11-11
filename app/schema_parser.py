import json
from .models import Field, Collection

#schema file is json file
#doc_counts: a dict like {"Product": 100000, "Stock": 20000000, ...}which tells how many documents each collection has

"""
@param schema_file: path to the JSON Schema file
@param doc_counts: dictionary with document counts per collection
@return: dictionary of Collection objects e.g {"Product": Collection(...), "Stock": Collection(...), ...}
"""

def parse_schema(schema_file: str, doc_counts: dict) -> dict:
    with open(schema_file) as f:
        schema = json.load(f)
    collections = {}

    """
    Iterate over each collection defined in the schema
    coll_name: name of the collection
    coll_def: definition of the collection in the schema

    e.g for "Product" collection:
    coll_name = "Product"
    coll_def = {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
                "brand": {"type": "string"},
                "description": {"type": "longstring"},
                "price": {"type": "number"},
                "currency": {"type": "string"},
                "vat_rate": {"type": "number"}
            }
        }
    }
    """
    for coll_name, coll_def in schema["properties"].items():

        fields = [] #list of Field objects for the collection
        props = coll_def.get("items", {}).get("properties", {})

        for fname, fdef in props.items():
            ftype = fdef.get("type", "string").lower() #default to string if type not specified
            fields.append(Field(name=fname, field_type=ftype))
            
        collections[coll_name] = Collection(name=coll_name, fields=fields, doc_count=doc_counts.get(coll_name, 0))
    return collections
