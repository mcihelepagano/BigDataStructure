"""
@param total_docs: Total number of documents in the collection.
@param distinct_keys: Number of distinct shard key values.
@param servers: Number of servers to distribute the data across (default: 1000).

@return: Dictionary with average documents and distinct key values per server.

"""

def sharding_stats(total_docs, distinct_keys, servers=1000):
    
    docs_per_server = total_docs / servers
    keys_per_server = distinct_keys / servers
    
    return {
        "docs_per_server": docs_per_server,
        "distinct_keys_per_server": keys_per_server
    }
