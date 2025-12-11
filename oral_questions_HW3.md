# Homework 3 – Oral Exam Preparation  
### Possible Questions & Model Answers  
### Big Data Structure – Operators, Sharding, Cost Model

This document contains a curated list of **likely oral questions** your professor may ask during the evaluation of Homework 3.  
Each question includes **short, clear, technically accurate answers**, so you can speak confidently and show full understanding of your code and the theory.

---

# 🔹 1. GENERAL QUESTIONS ABOUT THE PROJECT

### **Q1 — What is the purpose of Homework 3?**
Homework 3 focuses on implementing *operators* (Filter and Nested Loop Join) and computing their *costs* in a distributed, sharded NoSQL database.  
The goal is to automate:
- query size estimation  
- result size estimation  
- sharding-aware server selection  
- network volume  
- execution time (network + CPU)  
- CO₂ and price impact  

---

### **Q2 — What is the relationship between Homework 2 and Homework 3?**
Homework 2 builds the infrastructure:
- JSONSchema parsing  
- size computation  
- sharding statistics  

Homework 3 **uses** this structure to implement:
- filter operators  
- join operators  
- the professor’s cost model  

Without Homework 2, operators could not compute correct sizes or costs.

---

# 🔹 2. QUESTIONS ABOUT SHARDING

### **Q3 — When does a filter query contact only one server (S=1)?**
When the **sharding key appears inside the WHERE filter keys**.  
Example: If sharded by `IDP`, and query has `WHERE IDP = X`, then only one shard holds that data.

---

### **Q4 — What if the sharding key does NOT appear in the filters?**
The query becomes a **broadcast query**, meaning:

```
S = number_of_servers
```

This multiplies the cost dramatically.

---

### **Q5 — What makes a sharding key “good”?**
A good sharding key:
- has high cardinality  
- produces balanced distribution of documents  
- is frequently used in filters  
- reduces the number of contacted servers  

---

# 🔹 3. QUESTIONS ABOUT SELECTIVITY & CARDINALITY

### **Q6 — How is selectivity estimated?**
Default rule:

```
selectivity = 1 / Ndistinct(filter_key)
```

This means: if a key has many distinct values, each filter matches fewer documents.

---

### **Q7 — When does the result cardinality become exactly 1?**
When the filter fully specifies the **primary key** of the collection.

Example for Stock:
```
PK = (IDP, IDW)
WHERE IDP = ? AND IDW = ?
→ res_q = 1
```

---

### **Q8 — What if the filter contains multiple fields?**
Only the **first key** is used to estimate selectivity unless the PK is fully included.  
This is a simplification consistent with the project instructions.

---

# 🔹 4. QUESTIONS ABOUT QUERY SIZE (size_query)

### **Q9 — How do you compute size_query?**
Sum of all SELECT and WHERE fields:

```
size_query = Σ (12B + TYPE_SIZE)
```

Where:
- integer → 8B  
- string → 80B  
- longstring → 200B  

The overhead (12B) accounts for key + BSON-like metadata.

---

### **Q10 — Why do SELECT fields appear in size_query?**
Because a distributed DB must **transmit the projected attributes** to compute the final result.  
The operator cost is based on the data exchanged between servers.

---

# 🔹 5. QUESTIONS ABOUT OUTPUT SIZE (size_msg)

### **Q11 — What is size_msg?**
The size of **one document** returned by the query.

```
size_msg = Σ (12B + TYPE_SIZE(select_fields))
```

---

### **Q12 — Why does size_msg ignore WHERE attributes?**
WHERE fields are used only for filtering; they are **not part of the output** of the query.

---

# 🔹 6. NETWORK VOLUME QUESTIONS

### **Q13 — State the formula for vol_network.**

```
vol_network = S × size_query + res_q × size_msg
```

Where:
- `S`: number of contacted servers  
- `size_query`: cost to send query  
- `res_q`: number of returned documents  
- `size_msg`: size of each returned document  

---

### **Q14 — What happens to vol_network as S increases?**
It increases linearly:

```
double S → double network cost
```

---

### **Q15 — Why does join without sharding have the highest cost?**
Because the smaller relation must be **shipped entirely** to all servers, and the join result can be huge.

---

# 🔹 7. TIME COST QUESTIONS

### **Q16 — How do you compute execution time?**

```
time_network = vol_network / 100MB/s
time_cpu     = vol_network / 25GB/s
time_total   = time_network + time_cpu
```

---

### **Q17 — Which part dominates time for large joins?**
Network time always dominates over CPU time.

---

# 🔹 8. CO₂ AND PRICE QUESTIONS

### **Q18 — What is the CO₂ formula?**

```
CO2 = vol_GB × 0.011
```

### **Q19 — Why is the same coefficient used for price?**
The teacher defined a simplified model:

```
price = CO₂ = vol_GB × 0.011
```

So environmental cost and monetary cost grow proportionally to data transferred.

---

# 🔹 9. OPERATOR FUNCTION QUESTIONS

### **Q20 — What does filter_with_sharding() do?**

1. Compute S (1 or servers).  
2. Compute result_docs (selectivity or PK=1).  
3. Determine query fields types.  
4. Compute size_query and size_msg.  
5. Apply cost model.  
6. Return network/time/CO₂/price + result size.

---

### **Q21 — What does filter_without_sharding() do?**
Same as above but:

```
S = servers
```

Broadcast → expensive.

---

### **Q22 — What does nested_loop_without_sharding() do?**

1. Estimates join cardinality:
```
|R| × |S| / Ndistinct(join_key)
```
2. Moves the **smaller** relation over the network.  
3. Adds cost of transmitting join output.

---

### **Q23 — What does nested_loop_with_sharding() do?**

If sharded correctly:
- No input movement  
- Only join results are transmitted  

```
vol_network = result_docs × size_msg
```

---

# 🔹 10. CODE & ARCHITECTURE UNDERSTANDING

### **Q24 — Why is JSON Schema parsing required?**
Because all computations depend on:
- field types  
- nested structures  
- number of attributes  

The schema defines the structural cost of documents.

---

### **Q25 — Why are Field and Collection objects needed?**
They give a Python representation of:
- document structure  
- nested objects  
- arrays  
- field types  

Making size calculation automatic.

---

### **Q26 — Why is your system generalizable?**
Because operators depend only on:
- field types  
- filter keys  
- selectivity  
- document counts  
- sharding  

No part of the code is hardcoded to a specific DB.

---

# 🔹 11. ADVANCED ORAL QUESTIONS

### **Q27 — What happens if a filter contains a range (e.g., quantity > 10)?**
Selectivity would need a different model (not provided in HW3).  
A typical assumption is a uniform distribution, but HW3 keeps 1/Ndistinct.

---

### **Q28 — What happens with composite sharding keys?**
You evaluate S=1 only if **all** sharding key components are present in filters.

---

### **Q29 — Can this cost model be extended to hash joins or merge joins?**
Yes, by adding new operator classes and defining:
- input shipping  
- local computation  
- output shipping  

The structure is modular.

---

### **Q30 — Why is network volume the main bottleneck in distributed databases?**
Because network bandwidth is orders of magnitude slower than RAM bandwidth, so moving data dominates cost.

---

# END OF DOCUMENT  
Good luck for your oral exam!
