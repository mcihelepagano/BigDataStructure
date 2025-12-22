# examples/test_script.py

import os
import json

from app.schema_parser import parse_schema
from app.size_calculator import doc_size, bytes_to_gb
from app.sharding_analyzer import sharding_stats
from app.operators import (
    filter_with_sharding,
    filter_without_sharding,
    nested_loop_with_sharding,
    nested_loop_without_sharding,
)


BASE = os.path.dirname(__file__)


# ============================================================
# LOAD SCHEMA + STATS
# ============================================================

def load_environment(schema_name="schema_DB1.json", stats_name="stats_full.json"):
    schema_path = os.path.join(BASE, schema_name)
    stats_path = os.path.join(BASE, stats_name)

    with open(stats_path) as f:
        stats = json.load(f)

    db = parse_schema(schema_path, stats["doc_counts"], stats.get("array_hints", {}))
    collections = db.collections   # <-- unpack the Database

    return collections, stats


# ============================================================
# SHOW DOCUMENT SIZES
# ============================================================

def show_sizes(collections):
    print("\n=== DOCUMENT & COLLECTION SIZES ===")
    total_bytes = 0
    for name, coll in collections.items():
        dsize = doc_size(coll)
        csize = dsize * coll.doc_count
        total_bytes += csize
        print(f"{name:12s} | doc_size = {dsize:6.0f} B | collection = {bytes_to_gb(csize):10.3f} GB")
    print(f"TOTAL DB SIZE = {bytes_to_gb(total_bytes):.3f} GB")


# ============================================================
# SHOW SHARDING
# ============================================================

def show_sharding(stats):
    print("\n=== SHARDING STATS ===")

    dc = stats["doc_counts"]
    dv = stats["distinct_values"]
    servers = stats["servers"]

    table = [
        ("St-#IDP",      dc["Stock"],     dv["IDP"]),
        ("St-#IDW",      dc["Stock"],     dv["IDW"]),
        ("OL-#IDC",      dc["OrderLine"], dv["IDC"]),
        ("OL-#IDP",      dc["OrderLine"], dv["IDP"]),
        ("Prod-#IDP",    dc["Product"],   dv["IDP"]),
        ("Prod-#brand",  dc["Product"],   dv["brand"]),
    ]

    for label, total_docs, distinct in table:
        st = sharding_stats(total_docs, distinct, servers)
        print(f"{label:12s} -> docs/server={st['docs_per_server']:10.2f} | keys/server={st['distinct_keys_per_server']:10.2f}")


# ============================================================
# RUN SAMPLE OPERATORS (HW3)
# ============================================================

def run_operator_tests(collections, stats):
    print("\n=== OPERATOR TESTS (HW3) ===")

    stock = collections["Stock"]
    prod  = collections["Product"]
    order = collections["OrderLine"]
    distinct = stats["distinct_values"]
    servers  = stats["servers"]

    # ---------------------------------------------------------
    # FILTER WITH SHARDING — Example: Query 1 (IDP + IDW)
    # ---------------------------------------------------------
    print("\n>> Q1 FILTER WITH SHARDING (Stock by IDP, IDW)")

    q1 = filter_with_sharding(
        coll=stock,
        filter_keys=["IDP", "IDW"],
        select_fields=["quantity", "location"],
        sharding_key="IDP",
        distinct_values=distinct,
        servers=servers,
        pk_fields=["IDP", "IDW"],
    )

    print(f"size_query   = {q1.size_query} B")
    print(f"size_msg     = {q1.size_msg} B")
    print(f"vol_network  = {q1.vol_network} B")
    print(f"time_total   = {q1.time_total:.9f} s")
    print(f"CO2          = {q1.co2:.9f} kg")
    print(f"price        = {q1.price:.9f} €")

    # ---------------------------------------------------------
    # FILTER WITHOUT SHARDING — Example: Product by Brand
    # ---------------------------------------------------------
    print("\n>> Q2 FILTER WITHOUT SHARDING (Product by brand)")

    q2 = filter_without_sharding(
        coll=prod,
        filter_keys=["brand"],
        select_fields=["IDP", "name", "brand"],
        distinct_values=distinct,
        servers=servers,
    )

    print(f"size_query   = {q2.size_query} B")
    print(f"size_msg     = {q2.size_msg} B")
    print(f"vol_network  = {q2.vol_network} B")
    print(f"time_total   = {q2.time_total:.9f} s")
    print(f"CO2          = {q2.co2:.9f} kg")
    print(f"price        = {q2.price:.9f} €")

    # ---------------------------------------------------------
    # JOIN WITHOUT SHARDING — Example: Stock ⋈ Product on IDP
    # ---------------------------------------------------------
    print("\n>> Q3 NESTED LOOP JOIN WITHOUT SHARDING (Stock ⋈ Product)")

    q3 = nested_loop_without_sharding(
        left=stock,
        right=prod,
        join_key="IDP",
        distinct_values=distinct,
    )

    print(f"join result_docs = {q3['result_docs']}")
    print(f"vol_network      = {q3['vol_network']} B")
    print(f"time_total       = {q3['time_total']:.6f} s")
    print(f"CO2              = {q3['co2']:.6f} kg")
    print(f"price            = {q3['price']:.6f} €")

    # ---------------------------------------------------------
    # JOIN WITH SHARDING — ideal co-localized join
    # ---------------------------------------------------------
    print("\n>> Q4 NESTED LOOP JOIN WITH SHARDING (Stock ⋈ Product by IDP)")

    q4 = nested_loop_with_sharding(
        left=stock,
        right=prod,
        join_key="IDP",
        distinct_values=distinct,
        servers=servers,
    )

    print(f"join result_docs = {q4['result_docs']}")
    print(f"vol_network      = {q4['vol_network']} B")
    print(f"time_total       = {q4['time_total']:.6f} s")
    print(f"CO2              = {q4['co2']:.6f} kg")
    print(f"price            = {q4['price']:.6f} €")


# ============================================================
# MAIN
# ============================================================

def main():
    collections, stats = load_environment(
        schema_name="schema_DB1.json",
        stats_name="stats_full.json"
    )

    print("\n=== ENVIRONMENT LOADED SUCCESSFULLY ===")
    print("Collections:", list(collections.keys()))

    show_sizes(collections)
    show_sharding(stats)
    run_operator_tests(collections, stats)


if __name__ == "__main__":
    main()
