from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class Field:
    name: str
    field_type: str                  # integer, number, string, longstring, date, object, array
    subfields: List['Field'] = field(default_factory=list)
    avg_items: int = 1               # arrays: average number of items
    
    # JSON overhead per key: always 12 bytes
    overhead: int = 12
    
    # Base size depending on type: set by schema parser
    base_size: Optional[int] = None

    def is_object(self):
        return self.field_type == "object"

    def is_array(self):
        return self.field_type == "array"


@dataclass
class Collection:
    name: str
    fields: List[Field]
    doc_count: int = 0               # from statistics JSON


@dataclass
class Database:
    name: str
    collections: Dict[str, Collection]


@dataclass
class ShardingInfo:
    collection_name: str
    key: str
    cardinality: int
    distribution: Dict[str, int]     # key_value → count

