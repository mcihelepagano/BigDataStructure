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
        item = field.subfields[0]
        return field.avg_items * value_size(item)

    # fallback
    return TYPE_SIZES["string"]


def key_count(field):
    """
    Count HOW MANY KEYS contribute overhead.
    """
    # primitive
    if field.field_type in TYPE_SIZES:
        return 1

    # object
    if field.field_type == "object":
        return 1 + sum(key_count(f) for f in field.subfields)

    # array
    if field.field_type == "array":
        item = field.subfields[0]
        return 1 + field.avg_items * key_count(item)

    return 1


def doc_size(collection):
    """
    Professor’s formula:
    doc_size = sum(values) + sum(keys * 12)
    """
    total_value = sum(value_size(f) for f in collection.fields)
    total_keys = sum(key_count(f) for f in collection.fields)
    return total_value + total_keys * OVERHEAD


def collection_size(collection):
    return doc_size(collection) * collection.doc_count


def db_size(database):
    return sum(collection_size(c) for c in database.collections.values())


def bytes_to_gb(nbytes):
    return nbytes / (1024 ** 3)
