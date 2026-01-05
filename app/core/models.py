from dataclasses import dataclass, field
from typing import Dict, List, Optional

@dataclass
class Field:
    name: str
    field_type: str              # integer, number, string, longstring, date, object, array
    subfields: List['Field'] = field(default_factory=list)
    
    # CORREZIONE 1: Usa float per la media (es. 2.5 items)
    avg_items: float = 1.0       
    
    # Metodi helper utili per la leggibilità nel parser
    def is_object(self):
        return self.field_type == "object"

    def is_array(self):
        return self.field_type == "array"


@dataclass
class Collection:
    name: str
    fields: List[Field]
    doc_count: int = 0           # from statistics JSON
    
    # CORREZIONE 2: Aggiungi questo campo per salvare la dimensione calcolata
    # Verrà popolato dal main script usando size_calc.doc_size(self)
    doc_size: int = 0            


@dataclass
class Database:
    name: str
    collections: Dict[str, Collection]


@dataclass
class ShardingInfo:
    """
    Optional: Helper class to store sharding config if you parse it separately.
    """
    collection_name: str
    key: str
    cardinality: int
    distribution: Dict[str, int]     # key_value → count