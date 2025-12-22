"""
Sharding statistics for Homework 2.

This version is intentionally simple and matches exactly
the expectations of examples/test_script.py.
"""

def sharding_stats(total_docs, distinct_keys, servers=1000):
    """
    Compute simple average sharding statistics.

    @param total_docs: total number of documents for the collection
    @param distinct_keys: number of unique values of the sharding key
    @param servers: number of servers in the cluster (default: 1000)

    @return dict with:
        - docs_per_server
        - distinct_keys_per_server

    These keys MUST match what test_script.py expects.
    """

    docs_per_server = total_docs / servers
    keys_per_server = distinct_keys / servers

    return {
        "docs_per_server": docs_per_server,
        "distinct_keys_per_server": keys_per_server
    }
