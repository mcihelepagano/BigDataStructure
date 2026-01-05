# DVL Oral Preparation – Q&A (Big Data Structure Project)

---

## 1. JSON Schema and Parsing

**Q: What is a JSON Schema?**  
A JSON Schema is a structured description of the shape and type of data in a JSON document. It defines what fields exist, their data types (string, number, date, etc.), and how nested objects or arrays are organized. In this project, the schema describes each collection (like Product, Stock, Client) and their fields. It ensures consistency between different datasets and helps the program automatically compute document sizes.

**Q: How does your program parse the JSON Schema?**  
The program uses the `parse_schema()` function. It opens the JSON Schema file, loads it as a Python dictionary, and loops through each collection (found under `schema["properties"]`). For every field, it reads its name and type, then creates a `Field` object. Each collection becomes a `Collection` object that contains all its fields and a `doc_count` from the statistics file. Finally, the function returns a dictionary of all these collections.

**Q: What happens if a field type is missing in the schema?**  
The program automatically sets its type to `string`. This is done by the line:
```python
ftype = fdef.get("type", "string").lower()
```
So, if a field doesn’t specify a type, it defaults to `string` to avoid crashes. This is good practice for incomplete schemas.

---

## 2. Denormalization and Collections

**Q: What is denormalization?**  
Denormalization is the process of merging related entities into a single document to avoid complex join operations. Instead of storing separate tables (like in relational databases), we embed related information together. For example, embedding supplier and categories directly inside a product document.

**Q: Why is denormalization used in NoSQL databases?**  
Because NoSQL databases aim for scalability and fast reads. By keeping all related data in one document, we reduce the need for joins that are expensive in distributed systems. The trade-off is redundancy (some information may be duplicated), but read performance and scalability improve.

**Q: Can you give examples from your DB signatures (DB1 to DB4)?**  
- **DB1: `Prod{[Cat], Supp}, St, Wa, OL, Cl`** – Product includes embedded arrays of categories and a supplier object.  
- **DB2: `Prod{[Cat], Supp, [St]}, Wa, OL, Cl`** – The Stock information is now embedded inside Product documents.  
- **DB3: `St{Prod{[Cat], Supp}}, Wa, OL, Cl`** – Stock embeds Product information.  
- **DB4: `St, Wa, OL{Prod{[Cat], Supp}}, Cl`** – OrderLine embeds Product details.

Each variant trades space for fewer joins, depending on query frequency.

---

## 3. Computation Logic

**Q: How do you compute document sizes?**  
Each field has a base byte size according to its type and a 12-byte overhead for key-value storage. The rule is:
```
field_size = base_value_size + 12
```
You sum all field sizes for a document, multiply by the number of documents in the collection, and then sum all collections to get total database size.

**Q: Why is there an overhead of 12 bytes?**  
Because in JSON (and most storage formats), every key-value pair carries metadata: the field name, type information, and structural bytes. The 12-byte overhead approximates this cost.

**Q: Can you show an example calculation?**  
Sure. A Product document has 6 fields: `name`, `brand`, `description`, `price`, `currency`, `vat_rate`.
| Field | Type | Size (B) |
|--------|------|----------|
| name | string | 92 |
| brand | string | 92 |
| description | longstring | 212 |
| price | number | 20 |
| currency | string | 92 |
| vat_rate | number | 20 |
| **Total per document** |  | **528 B** |
For 100,000 products: 528 × 100,000 = 52,800,000 B ≈ **0.049 GB**.

**Q: What are the type-size rules used?**  
| Type | Size (B) |
|------|-----------|
| Integer/Number | 8 |
| String | 80 |
| Date | 20 |
| LongString | 200 |
| Overhead | +12 per key-value pair |

---

## 4. Sharding and Distribution

**Q: What is a sharding key?**  
The sharding key is the field that determines how documents are distributed across servers in a cluster. The database uses it to decide which server (or shard) stores each document. For example, if the sharding key is `client_id`, all orders for the same client go to the same shard.

**Q: What is the difference between `docs_per_server` and `distinct_keys_per_server`?**  
They measure two different aspects of distribution:
- `docs_per_server` = total number of documents stored per server (data volume)
- `distinct_keys_per_server` = number of unique sharding key values per server (key-space coverage)

Example:
| Metric | Formula | Example (OL-#IDC) | Meaning |
|---------|----------|-------------------|----------|
| Docs/server | 4×10⁹ / 1000 = 4,000,000 | 4 million order lines stored on each server |
| Keys/server | 10⁷ / 1000 = 10,000 | Each server handles 10,000 unique clients |

Each client (key) has about 400 orders on average. These metrics help evaluate if a sharding key balances load evenly.

**Q: Why can two sharding strategies produce very different results?**  
Because some fields have more distinct values than others. For example, sharding Stock by `warehouse_id` (`St-#IDW`) creates only 200 shards since there are only 200 warehouses, while sharding by `product_id` (`St-#IDP`) balances data across 100,000 products. The first is uneven (many empty servers), the second is well distributed.

**Q: What does the `sharding_stats()` function do?**  
It takes three parameters: total documents, distinct keys, and number of servers (default 1000). It returns:
```
{
  "docs_per_server": total_docs / servers,
  "distinct_keys_per_server": distinct_keys / servers
}
```
It helps simulate and compare different sharding strategies for balance.

---

## 5. Code and Package Structure

**Q: How is your package organized?**  
It follows a clean modular structure:
- `models.py`: defines `Field`, `Collection`, and `Database` classes.
- `parsers/json_schema.py`: reads a JSON Schema and builds a structure of Collection objects.
- `core/size_calc.py`: calculates document, collection, and database sizes.
- `sharding_analyzer.py`: computes sharding distribution statistics.
- `examples/`: contains example schemas, stats, and test scripts.

**Q: How do these modules work together?**  
`json_schema` reads the schema and creates data structures. These are passed to `size_calc`, which computes sizes, and optionally to `sharding_analyzer`, which simulates distribution. The `examples/test_script.py` file demonstrates how to chain all these steps.

**Q: What makes this design modular and reusable?**  
Each module performs one job and can be imported independently. For example, another program could reuse only `size_calc` to estimate storage without re-parsing the schema. This is good software design because it isolates concerns and supports future extension.

**Q: How would you extend the program?**  
I could add functions to compute:
- Average data volume per shard in GB.
- Index storage overhead.
- Compression ratio simulations.
I could also add a CLI or web interface that takes any JSON Schema and instantly outputs these results.

---

## 6. General Theory

**Q: Why is denormalization common in NoSQL systems?**  
Because NoSQL databases are optimized for scalability and read performance. Denormalization avoids joins by grouping related data in the same document, reducing network overhead. It’s a trade-off: you gain speed but may lose some consistency.

**Q: What is the difference between horizontal and vertical scaling?**  
- **Vertical scaling:** adding more power (CPU, RAM) to a single server.
- **Horizontal scaling:** adding more servers (shards) to distribute data and load.
Sharding is a form of horizontal scaling.

**Q: How does sharding improve performance?**  
It splits a massive dataset across many servers, so each server only handles a fraction of queries and storage. This allows linear scalability: doubling servers roughly halves the load per server.

**Q: What is the relationship between schema design and query performance?**  
Schema design directly affects how fast queries can find and return data. A well-designed schema groups data the same way queries need it. For example, embedding product details in OrderLine improves query performance for order reports, since no external lookup is needed.

---

## 7. Tips for Oral Defense

- **Connect concepts clearly:** When asked about a function, mention the file name and purpose (e.g., *"In `parsers/json_schema.py`, I read the JSON Schema and create objects for each collection."*)
- **Use transitions:** If a question jumps from theory to implementation, bridge them: *"That concept is reflected in my code by..."*
- **When unsure:** Rephrase the question and show reasoning: *"If I understand correctly, you're asking about how sharding distributes data. In my program, I simulate that using..."*
- **Emphasize automation:** *"The goal of my package was to automate manual calculations like document size and sharding distribution."*
- **Show understanding, not memorization:** Speak naturally and refer to examples (like OL-#IDC or Product size). This demonstrates comprehension.

---

