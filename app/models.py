from dataclasses import dataclass, field
from typing import Dict, List

@dataclass
class Field:
    name: str
    field_type: str              # integer, string, longstring, date, array, object
    subfields: List['Field'] = field(default_factory=list)

@dataclass
class Collection:
    name: str
    fields: List[Field]
    doc_count: int = 0

@dataclass
class Database:
    collections: Dict[str, Collection]
