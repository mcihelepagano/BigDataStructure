TYPE_SIZES = {
    "integer": 8,
    "number": 8,
    "string": 80,
    "longstring": 200,
    "date": 20
}
OVERHEAD = 12

def field_size(field):
    base = TYPE_SIZES.get(field.field_type, 80)
    return base + OVERHEAD

def doc_size(collection):
    """Compute document size in bytes."""
    return sum(field_size(f) for f in collection.fields)

def collection_size(collection):
    """Compute collection size in bytes."""
    return doc_size(collection) * collection.doc_count

def db_size(database):
    """Compute database total size in bytes."""
    return sum(collection_size(c) for c in database.collections.values())

def bytes_to_gb(nbytes):
    return nbytes / (1024 ** 3)
