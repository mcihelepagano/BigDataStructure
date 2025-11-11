## 1. Introduction
This report documents the creation and analysis of the **DVL Analyzer**, a Python-based tool that automates the computation of **document**, **collection**, and **database** sizes using **JSON Schemas** and dataset statistics.  
It was developed for the **Big Data Structure** course at ESILV to explore data volume estimation in **NoSQL denormalized models**.

---

## 2. Package Overview
The package `dvl_analyzer` is composed of modular files, each performing a distinct function for schema parsing, size computation, or sharding analysis.  
The modular design allows for reusability and testing with different JSON Schemas.

###  Structure
| File | Description |
|------|--------------|
| **models.py** | Defines data structures (`Field`, `Collection`, `Database`). |
| **schema_parser.py** | Parses JSON Schemas and constructs Python objects. |
| **size_calculator.py** | Implements all document and collection size computation logic. |
| **sharding_analyzer.py** | Computes document and key distribution across servers. |
| **examples/** | Contains test schemas and scripts for validation. |

---

## 3. Computation Logic

Document and database sizes are computed using field-size rules provided during the practice sessions.  
Each field contributes its **base value size** plus an **overhead of 12 bytes** to represent key-value pairs.

### Field Size Rules

| Type | Size (Bytes) |
|------|---------------|
| Integer / Number | 8 |
| String | 80 |
| Date | 20 |
| LongString | 200 |
| Overhead | +12 bytes per key-value or array element |

### Computation Steps
1. **Field size:** `field_size = base_value_size + 12`
2. **Document size:** sum of all field sizes
3. **Collection size:** `doc_size × number_of_documents`
4. **Database size:** sum of all collection sizes
5. **Conversion:** `1 GB = 1,073,741,824 bytes`

### Example
A **Product** document contains 6 fields:  
`name`, `brand`, `description`, `price`, `currency`, and `vat_rate`.

| Field | Type | Size (B) |
|--------|------|----------|
| name | string | 92 |
| brand | string | 92 |
| description | longstring | 212 |
| price | number | 20 |
| currency | string | 92 |
| vat_rate | number | 20 |
| **Total** |  | **528 B / doc** |

For 100,000 products:
```
528 × 100,000 = 52,800,000 bytes ≈ 0.049 GB
```

---

## 4. Sharding Analysis

The analyzer also computes the **distribution of documents** across a cluster of 1,000 servers using a chosen **sharding key**.

### Formulas
```
docs_per_server = total_documents / number_of_servers
distinct_keys_per_server = distinct_shard_keys / number_of_servers
```

### Example
For the **Stock** collection:
- Total documents: 20,000,000  
- Distinct Product IDs: 100,000  
- Servers: 1,000  

```
docs_per_server = 20,000,000 / 1,000 = 20,000
keys_per_server = 100,000 / 1,000 = 100
```

For **OrderLine** sharded by Client ID:
```
docs_per_server = 4×10⁹ / 1,000 = 4,000,000
keys_per_server = 10⁷ / 1,000 = 10,000
```

---

## 5. Results and Interpretation

The analyzer was tested using the **DB1 JSON Schema** and provided statistics.  
Below are the results for average document and collection sizes.

| Collection | Doc Size (B) | Total Size (GB) |
|-------------|--------------|-----------------|
| Product | 712 | 0.066 |
| Stock | 388 | 7.23 |
| Warehouse | 204 | 0.000 |
| OrderLine | 592 | 2205 |
| Client | 584 | 5.44 |

### Example Calculation
If an `OrderLine` document has 7 fields:
```
size_per_doc = 592 bytes
collection_size = 592 × 4×10⁹ = 2.368×10¹² bytes ≈ 2205 GB
```
This shows that **OrderLine** dominates the total storage (~2.22 TB), which is typical for transactional datasets.

---

## 6. Conclusion

The **DVL Analyzer** automates all required computations for document and database sizing.  
It parses JSON Schemas, applies size estimation rules, and computes sharding statistics.  
This provides a **reproducible and extendable framework** for evaluating the impact of **NoSQL denormalization** and sharding strategies.

---

## Example Execution

Run the test example from your project root:
```bash
py -m examples.test_script
```

Example output:
```
=== Document and Collection Sizes ===
Product    :   712 B/doc | 0.066 GB total
Stock      :   388 B/doc | 7.23 GB total
Warehouse  :   204 B/doc | 0.000 GB total
OrderLine  :   592 B/doc | 2205.372 GB total
Client     :   584 B/doc | 5.439 GB total

=== Sharding Examples ===
Stock by Product ID: {'docs_per_server': 20000.0, 'distinct_keys_per_server': 100.0}
OrderLine by Client ID: {'docs_per_server': 4000000.0, 'distinct_keys_per_server': 10000.0}
Product by Brand: {'docs_per_server': 100.0, 'distinct_keys_per_server': 5.0}
```

---
