import json, os, sys

#sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.size_calculator import doc_size, collection_size, bytes_to_gb
from app.schema_parser import parse_schema
from app.sharding_analyzer import sharding_stats

BASE = os.path.dirname(__file__)
with open(os.path.join(BASE, "stats.json")) as f:
    stats = json.load(f)

collections = parse_schema(os.path.join(BASE, "schema_DB1.json"), stats)

print("=== Document and Collection Sizes ===")
for name, coll in collections.items():
    d_size = doc_size(coll)
    c_size = collection_size(coll)
    print(f"{name:10s}: {d_size:>6.0f} B/doc | {bytes_to_gb(c_size):.3f} GB total")

print(f"\nTotal DB size: {bytes_to_gb(sum(collection_size(c) for c in collections.values())):.2f} GB")

print("\n=== Sharding Examples ===")
print("Stock by Product ID:", sharding_stats(2e7, 1e5))
print("OrderLine by Client ID:", sharding_stats(4e9, 1e7))
print("Product by Brand:", sharding_stats(1e5, 5e3))
