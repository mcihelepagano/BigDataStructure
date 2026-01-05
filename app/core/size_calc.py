TYPE_SIZES = {
    "integer": 8,
    "number": 8,
    "string": 80,
    "longstring": 200,
    "date": 20
}

OVERHEAD = 12


def value_size(field):
    """
    Computes ONLY the value size of a field:
    - primitive: TYPE_SIZES
    - object: sum of subfield values
    - array: avg_items * value of item
    """
    # primitive
    if field.field_type in TYPE_SIZES:
        return TYPE_SIZES[field.field_type]

    # object
    if field.field_type == "object":
        return sum(value_size(f) for f in field.subfields)

    # array
    if field.field_type == "array":
        if not field.subfields:
            return 0
        item = field.subfields[0]
        return field.avg_items * value_size(item)

    # fallback
    return TYPE_SIZES["string"]


def key_count(field):
    """
    Count JSON keys contributing overhead:
    - primitive: 1 key
    - object: 1 for the object + child keys
    - array: 1 for the array + child keys (counted ONCE, not multiplied by length)
    """
    # primitive
    if field.field_type in TYPE_SIZES:
        return 1

    # object
    if field.field_type == "object":
        return 1 + sum(key_count(f) for f in field.subfields)

    # array
    if field.field_type == "array":
        # CORREZIONE: Scendiamo ricorsivamente nel tipo contenuto nell'array
        # per contare le sue chiavi (es. ID, Qty) almeno una volta.
        if field.subfields:
            return 1 + key_count(field.subfields[0])
        return 1

    return 1


def doc_size(collection):
    """
    Professor’s formula:
    doc_size = sum(values) + sum(keys * 12)
    """
    total_value = sum(value_size(f) for f in collection.fields)
    # Nota: qui assumiamo che collection.fields sia una lista di campi 'root'.
    # key_count restituirà 1 per ogni campo root + i suoi figli.
    total_keys = sum(key_count(f) for f in collection.fields)
    return total_value + total_keys * OVERHEAD


def collection_size(collection):
    return doc_size(collection) * collection.doc_count


def db_size(database):
    return sum(collection_size(c) for c in database.collections.values())


def bytes_to_gb(nbytes):
    return nbytes / (10 ** 9) # Corretto (Base 10)