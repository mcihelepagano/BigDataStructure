import argparse
import json
import os

from app.size_calculator import doc_size, collection_size, bytes_to_gb
from app.schema_parser import parse_schema
from app.sharding_analyzer import sharding_stats

BASE = os.path.dirname(__file__)


def load_stats(stats_path):
    with open(stats_path) as f:
        data = json.load(f)
    if "doc_counts" in data:
        return (
            data.get("doc_counts", {}),
            data.get("distinct_values", {}),
            data.get("array_hints", {}),
            data.get("servers", 1000),
        )
    return data, {}, {}, 1000


def print_sizes(collections):
    print("=== Document and Collection Sizes ===")
    total_bytes = 0
    for name, coll in collections.items():
        d_size = doc_size(coll)
        c_size = collection_size(coll)
        total_bytes += c_size
        print(f"{name:12s}: {d_size:>8.0f} B/doc | {bytes_to_gb(c_size):10.3f} GB total")
    print(f"\nTotal DB size: {bytes_to_gb(total_bytes):.3f} GB")


def print_sharding(doc_counts, distinct, servers):
    print("\n=== Sharding Scenarios (avg per server) ===")

    def row(label, total_docs, distinct_key_values):
        stats = sharding_stats(total_docs, distinct_key_values, servers)
        return f"{label:12s} -> docs/server: {stats['docs_per_server']:>12.2f} | keys/server: {stats['distinct_keys_per_server']:>12.2f}"

    st_docs = doc_counts.get("Stock", 0)
    ol_docs = doc_counts.get("OrderLine", 0)
    prod_docs = doc_counts.get("Product", 0)

    prod_count  = distinct.get("IDP", 100000)
    wh_count    = distinct.get("IDW", 200)
    client_count = distinct.get("IDC", 10000000)
    brand_count = distinct.get("brand", 5000)

    print(row("St-#IDP",   st_docs, prod_count))
    print(row("St-#IDW",   st_docs, wh_count))
    print(row("OL-#IDC",   ol_docs, client_count))
    print(row("OL-#IDP",   ol_docs, prod_count))
    print(row("Prod-#IDP", prod_docs, prod_count))
    print(row("Prod-#brand", prod_docs, brand_count))


def main():
    parser = argparse.ArgumentParser(description="Compute document/collection/DB sizes and sharding stats.")
    parser.add_argument("--schema", default=os.path.join(BASE, "schema_DB1.json"), help="Path to JSON schema")
    parser.add_argument("--stats", default=os.path.join(BASE, "stats_full.json"), help="Path to stats file")
    args = parser.parse_args()

    doc_counts, distinct, array_hints, servers = load_stats(args.stats)

    db = parse_schema(args.schema, doc_counts, array_hints)
    collections = db.collections

    print(f"Schema: {args.schema}")
    print(f"Database name: {db.name}")
    print(f"Stats : {args.stats}")

    print_sizes(collections)
    print("DEBUG DOC COUNTS:", doc_counts)

    print_sharding(doc_counts, distinct, servers)


if __name__ == "__main__":
    main()
