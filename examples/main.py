# examples/main.py

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
# LOAD ENVIRONMENT
# ============================================================

def load_environment(schema="schema_DB1.json", stats="stats_full.json"):
    schema_path = os.path.join(BASE, schema)
    stats_path  = os.path.join(BASE, stats)

    with open(stats_path) as f:
        stats = json.load(f)

    db = parse_schema(schema_path, stats["doc_counts"], stats.get("array_hints"))
    collections = db.collections

    return collections, stats


# ============================================================
# PRINT HELPERS
# ============================================================

def print_cost_block(label: str, cost, servers_working: int, servers_total: int):
    print(label)
    print(f"  Query size    = {cost.size_query} B")
    print(f"  Msg size      = {cost.size_msg} B")
    print(f"  Result docs   = {cost.result_docs}")
    print(f"  Result size   = {cost.result_size_bytes} B")
    print(f"  Network vol   = {cost.vol_network} B")
    print(f"  RAM volume    = {cost.ram_volume} B (per working shard + output)")
    print(f"  RAM total     = {cost.ram_volume_total} B (servers working = {servers_working}, total shards = {servers_total})")
    print(f"  Total time    = {cost.time_total:.9f} s")
    print(f"  COO           = {cost.co2:.9f} kg")
    print(f"  Price         = {cost.price:.9f} EUR")


def print_join_block(label: str, res: dict, outer_label: str, inner_label: str):
    print(label)
    print(f"Result docs      = {res['result_docs']}")
    print(f"Network vol      = {res['vol_network']} B")
    print(f"RAM volume       = {res['ram_volume']} B")
    if "ram_volume_alt" in res:
        print(f"RAM volume (alt) = {res['ram_volume_alt']} B")
        if "ram_volume_alt_outer" in res:
            print(f"  alt outer      = {res['ram_volume_alt_outer']} B")
        if "ram_volume_alt_inner" in res:
            print(f"  alt inner      = {res['ram_volume_alt_inner']} B")
    print(f"Total time       = {res['time_total']:.6f} s")
    print(f"COO              = {res['co2']:.6f} kg")
    print(f"Price            = {res['price']:.6f} EUR")
    print(f"  Outer ({outer_label}):")
    print(f"    result_docs  = {res['outer'].result_docs}")
    print(f"    vol_network  = {res['outer'].vol_network} B")
    print(f"    ram_total    = {res['outer'].ram_volume_total} B")
    print(f"    time_total   = {res['outer'].time_total:.6f} s")
    print(f"  Inner per iter ({inner_label}):")
    print(f"    result_docs  = {res['inner_per_iteration'].result_docs}")
    print(f"    vol_network  = {res['inner_per_iteration'].vol_network} B")
    print(f"    ram_total    = {res['inner_per_iteration'].ram_volume_total} B")
    print(f"    time_total   = {res['inner_per_iteration'].time_total:.6f} s")


# ============================================================
# HOMEWORK 2: SIZES + SHARDING
# ============================================================

def homework2(collections, stats):

    print("\n=== HOMEWORK 2 - DOCUMENT & COLLECTION SIZES ===")
    total_bytes = 0

    for name, coll in collections.items():
        dsize = doc_size(coll)
        csize = dsize * coll.doc_count
        total_bytes += csize
        print(f"{name:12s} | doc_size={dsize:6.0f} B | collection={bytes_to_gb(csize):10.3f} GB")

    print(f"\nTOTAL DB SIZE = {bytes_to_gb(total_bytes):.3f} GB")

    print("\n=== HOMEWORK 2 - SHARDING STATS ===")
    dc = stats["doc_counts"]
    dv = stats["distinct_values"]
    servers = stats["servers"]

    rows = [
        ("St-#IDP",      dc["Stock"],     dv["IDP"]),
        ("St-#IDW",      dc["Stock"],     dv["IDW"]),
        ("OL-#IDC",      dc["OrderLine"], dv["IDC"]),
        ("OL-#IDP",      dc["OrderLine"], dv["IDP"]),
        ("Prod-#IDP",    dc["Product"],   dv["IDP"]),
        ("Prod-#brand",  dc["Product"],   dv["brand"]),
    ]

    for label, total_docs, distinct in rows:
        st = sharding_stats(total_docs, distinct, servers)
        print(f"{label:12s} | docs/server={st['docs_per_server']:10.2f} | keys/server={st['distinct_keys_per_server']:10.2f}")


# ============================================================
# HOMEWORK 3: OPERATOR COSTS
# ============================================================

def homework3(collections, stats):
    print("\n=== HOMEWORK 3 - OPERATOR COSTS ===")

    stock = collections["Stock"]
    prod  = collections["Product"]
    distinct = stats["distinct_values"]
    servers  = stats["servers"]

    print("\n>> Q1 - FILTER WITH SHARDING (Stock WHERE IDP,IDW)")
    q1 = filter_with_sharding(
        coll=stock,
        filter_keys=["IDP", "IDW"],
        select_fields=["IDP", "quantity", "location"],
        sharding_key="IDP",
        distinct_values=distinct,
        servers=servers,
        pk_fields=["IDP", "IDW"]
    )
    print_cost_block("Query metrics:", q1, servers_working=1, servers_total=servers)

    print("\n>> Q2 - FILTER WITHOUT SHARDING (Product WHERE brand)")
    apple_selectivity = 50 / prod.doc_count if prod.doc_count else 0
    apple_servers_working = 50
    q2 = filter_without_sharding(
        coll=prod,
        filter_keys=["brand"],
        select_fields=["IDP", "name", "amount"],
        distinct_values=distinct,
        servers=servers,
        selectivity=apple_selectivity,
        servers_working=apple_servers_working
    )
    print_cost_block("Query metrics:", q2, servers_working=apple_servers_working, servers_total=servers)

    print("\n>> Q2b - FILTER WITH SHARDING ON BRAND (Product WHERE brand)")
    q2b = filter_with_sharding(
        coll=prod,
        filter_keys=["brand"],
        select_fields=["IDP", "name", "amount"],
        sharding_key="brand",
        distinct_values=distinct,
        servers=servers,
        selectivity=apple_selectivity,
        servers_working=1
    )
    print_cost_block("Query metrics:", q2b, servers_working=1, servers_total=servers)


# ============================================================
# JOIN QUERIES
# ============================================================

def joins_section(collections, stats):
    print("\n=== JOIN QUERIES - NESTED LOOP EXAMPLES ===")

    stock = collections["Stock"]
    prod  = collections["Product"]
    distinct = stats["distinct_values"]
    servers  = stats["servers"]

    j1 = nested_loop_without_sharding(
        left=stock,
        right=prod,
        join_key="IDP",
        distinct_values=distinct,
        outer_filter_keys=["IDW"],
        outer_select_fields=["IDP", "quantity"],
        inner_select_fields=["name"]
    )
    print_join_block("\n>> J1 - NESTED LOOP JOIN WITHOUT SHARDING (Stock ⋈ Product ON IDP)", j1, "Stock", "Product")

    print("\n>> J2 - NESTED LOOP JOIN WITH SHARDING (Stock ⋈ Product ON IDP) -- scenarios")
    scenarios = [
        ("Outer sharding IDW, Inner sharding IDP", "IDW", "IDP"),
        ("Outer sharding IDP, Inner sharding IDP", "IDP", "IDP"),
        ("Outer sharding IDW, Inner sharding brand", "IDW", "brand"),
    ]
    for label, outer_shard, inner_shard in scenarios:
        print(f"\n-- {label} --")
        j2 = nested_loop_with_sharding(
            left=stock,
            right=prod,
            join_key="IDP",
            distinct_values=distinct,
            servers=servers,
            outer_filter_keys=["IDW"],
            outer_select_fields=["IDP", "quantity"],
            inner_select_fields=["name"],
            outer_sharding_key=outer_shard,
            inner_sharding_key=inner_shard
        )
        print_join_block("", j2, "Stock", "Product")


# ============================================================
# PLACEHOLDER FOR HOMEWORK 4
# ============================================================

def homework4(collections, stats):
    print("\n=== HOMEWORK 4 - NOT IMPLEMENTED YET ===")
    print("Query optimization, cost plans, estimated best operators... coming soon!")


# ============================================================
# MENU
# ============================================================

def main():
    collections, stats = load_environment()

    while True:
        print("\n========================")
        print("     HOMEWORK MENU")
        print("========================")
        print("1 - Homework 2 (Sizes + Sharding)")
        print("2 - Homework 3 (Operators)")
        print("3 - Join Queries (Nested Loop)")
        print("4 - Homework 4 (Coming soon)")
        print("0 - Exit")

        choice = input("\nChoose an option: ").strip()

        if choice == "1":
            homework2(collections, stats)

        elif choice == "2":
            homework3(collections, stats)

        elif choice == "3":
            joins_section(collections, stats)

        elif choice == "4":
            homework4(collections, stats)

        elif choice == "0":
            print("Goodbye!")
            break

        else:
            print("Invalid choice. Try again.")


if __name__ == "__main__":
    main()
