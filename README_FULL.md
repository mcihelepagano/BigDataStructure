# BigDataStructure – Homework Automations (Schema Parsing, Size Estimation, Sharding & Operators)

This package implements the full set of requirements from the Big Data Structure coursework:
- JSON schema parsing  
- document / collection / database size computation  
- sharding statistics  
- operator-level cost estimation (filter + join)  
- professor’s cost model (network volume, time, CO₂, price)

The system is completely general:  
it works for any JSONSchema and any set of queries defined in terms of:
- SELECT fields  
- WHERE filter keys  
- sharding key  
- primary key (optional)  
- distinct_values statistics  

---

# 📁 Project Structure

```
BigDataStructure/
│
├── app/
│   ├── core/models.py              
│   ├── parsers/json_schema.py       
│   ├── core/size_calc.py     
│   ├── sharding_analyzer.py   
│   ├── cost_model/formulas.py      
│   ├── cost_model/operations.py           
│
├── examples/
│   ├── schema_DB1.json
│   ├── schema_DB2.json
│   ├── schema_DB3.json
│   ├── schema_DB4.json
│   ├── schema_DB5.json
│   ├── stats_full.json
│   └── test_script_hw3.py     
│
└── README.md
```

---

# 1. JSON Schema Parsing (`parsers/json_schema.py`)

The parser converts a JSONSchema into internal Python objects (`Collection`, `Field`).  
It supports nested objects, arrays, and custom type sizes (integer, string, longstring, date).

---

# 2. Size Calculation (`core/size_calc.py`)

Document size uses the professor’s formula:

```
field_size = 12B (overhead) + type_size
```

Collection size and DB size are computed automatically.

---

# 3. Sharding Analysis (`sharding_analyzer.py`)

Computes:

- docs/server  
- keys/server  

Used to evaluate whether a sharding key is well balanced.

---

# 4. Cost Model (`cost_model/formulas.py`)

Implements:

- query size  
- message size  
- network volume  
- time_network  
- time_cpu  
- time_total  
- CO₂ and price  

Using the professor’s constants (100 MB/s, 25 GB/s, etc.).

---

# 5. Operators (`cost_model/operations.py`)

Implements:

- filter_without_sharding  
- filter_with_sharding  
- nested_loop_without_sharding  
- nested_loop_with_sharding  

Each operator returns `CostOutput` containing all cost metrics.

---

# 6. Test Script

Run with:

```
python -m examples.test_script_hw3
```

---

# 7. Usage Example

```python
result = filter_with_sharding(
    coll=collections["Stock"],
    filter_keys=["IDP", "IDW"],
    select_fields=["quantity", "location"],
    sharding_key="IDP",
    distinct_values=stats["distinct_values"],
    servers=stats["servers"],
    pk_fields=["IDP", "IDW"]
)
```

---

# 8. Oral Exam Summary

Explain:

- document size formula  
- sharding effect on S  
- PK lookup meaning (res_q = 1)  
- network volume computation  
- join costs  

---

# 9. Conclusion

This package fully automates HW2 + HW3 and provides a modular framework for evaluating NoSQL performance.

---

# 10. Examples & Detailed Calculations

## 10.1 Field Size Model

```
integer    = 12 + 8  = 20B  
string     = 12 + 80 = 92B  
longstring = 12 + 200 = 212B  
date       = 12 + 20 = 32B
```

---

# 10.2 FILTER WITH SHARDING — Example Q1

Query:

```
SELECT quantity, location
FROM Stock
WHERE IDP = X AND IDW = Y
```

### Involved fields:

```
quantity  → integer → 20B  
location  → string  → 92B  
IDP       → integer → 20B  
IDW       → integer → 20B  
```

### size_query:

```
20 + 92 + 20 + 20 = 152B
```

### size_msg:

```
20 + 92 = 112B
```

### Sharding:

```
sharding_key = IDP
filter_keys include IDP → S = 1
```

### Result cardinality:

Stock PK = (IDP, IDW):

```
res_q = 1
```

### Network volume:

```
vol_network = 1 × 152 + 1 × 112 = 264B
```

---

# 10.3 FILTER WITHOUT SHARDING — Example

Query:

```
SELECT IDP, name, brand
FROM Product
WHERE brand = X
```

### size_query:

```
IDP = 20B  
name = 92B  
brand = 92B  
(brand WHERE) = 92B  
----------------------
size_query = 296B
```

### Result estimate:

```
res_q = N(Product)/Ndistinct(brand)
= 100000 / 5000
= 20 docs
```

### Broadcasting to 1000 servers:

```
S = 1000
```

### vol_network:

```
1000 × 296 + 20 × 204 = 300080B
```

---

# 10.4 NESTED LOOP JOIN WITHOUT SHARDING

Join:

```
Stock ⋈ Product ON IDP
```

### Cardinality:

```
result_docs = |Stock| × |Product| / Ndistinct(IDP)
             = (20M × 100k) / 100k
             = 20M
```

### Message size:

```
doc_stock   = 296B
doc_product = 1224B
size_msg    = 1520B
```

### Network volume:

Move smaller relation (Product):

```
Product_bytes = 100k × 1224B
vol_network = Product_bytes + result_docs × size_msg
```

---

# 10.5 NESTED LOOP JOIN WITH SHARDING

If sharded on IDP:

```
vol_network = result_docs × size_msg
```

No relation shipping is required.

---

# 10.6 What Each Function Does

### parse_schema()
Reads JSON schema → builds Collection objects with types and subfields.

### doc_size()
Computes byte size of a single document.

### collection_size()
Computes total storage of a collection.

### db_size()
Sums all collections.

### sharding_stats()
Computes docs/server and keys/server.

### filter_with_sharding()
Implements:
- S = 1 if sharding key is in filters  
- result cardinality (PK lookup or selectivity)  
- cost model application  

### filter_without_sharding()
Same as above but:
- S = servers  
- full broadcast  

### nested_loop_without_sharding()
Computes:
- join cardinality  
- movement of smaller relation  
- result shipping cost  

### nested_loop_with_sharding()
Assumes perfect sharding:
- no movement of inputs  
- only result transmission  

---

This expanded section provides step-by-step numerical examples for understanding all internal computations of the system.
