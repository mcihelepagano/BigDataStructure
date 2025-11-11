def sharding_stats(total_docs, distinct_keys, servers=1000):
    """Return average documents and distinct key values per server."""
    docs_per_server = total_docs / servers
    keys_per_server = distinct_keys / servers
    return {
        "docs_per_server": docs_per_server,
        "distinct_keys_per_server": keys_per_server
    }
