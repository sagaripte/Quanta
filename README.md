# A Ultra-Fast, Memory-Efficient Data Engine for Analytical Workloads

## Key Features & Benefits

* **Blazing-Fast Queries:** Millisecond-level query execution on millions of rows (see benchmarks!).
* **Efficient Data Ingestion:** Optimized memory management allows rapid loading & appending of data.
* **Ultra-Low Memory Usage:** Uses raw memory allocation & dictionary encoding (~650MB for 20M records).
* **Single-JVM Performance:** No need for distributed systems or in-memory data grids.
* **Persistent & Crash-Resilient:** Memory-mapped files enable instant reloads & durability.
* **Optimized for Analytics:** Handles millions of records, ideal for read-heavy queries.
* **Real-Time & Batch Processing:** Supports incremental updates and low-latency reads.
* **Zero Third-Party Dependencies:** Pure Java implementation with no external libraries required.

## How Quanta Works

Quanta bypasses Java's garbage collection by directly managing memory, minimizing footprint and eliminating GC pauses.  It uses:

* **Raw Memory Allocation:**  Manages memory outside the Java heap for optimal control.
* **Memory-Mapped Files:** Provides fast persistence, instant reloads, and efficient data access.
* **Dictionary-Based Compression:** Compresses repeated values for efficient storage of string data.
* **Bitset Indexing:** Enables ultra-fast query execution.

## Quick Start Example (Ad Event Analysis)

```java
// 1. Define the dataset schema
Quanta adEvents = new QuantaBuilder("AdEvents", "/data/ad_tracking")
    .addPrimaryKeyIntColumn("event_id")
    .addStringColumn("campaign", 30, IndexCardinality.TINY)
    .addDictionaryColumn("region", 10)
    .addDictionaryColumn("device", 10)
    .addIntColumn("year", IndexCardinality.TINY)
    .addIntColumn("month", IndexCardinality.TINY)
    .addIntColumn("day", IndexCardinality.TINY)
    .addIntegerFact("impressions")
    .addIntegerFact("clicks")
    .addFact("bid_price")
    .getQuanta();

// 2. Insert 20 Million Records 
Random rand = new Random();
String[] campaigns = {/* ... */}; // campaign data
String[] regions = {/* ... */};    // region data
String[] devices = {/* ... */};    // device data

for (int i = 0; i < 20_000_000; i++) {
    Tuple row = new Tuple();
    // ... populate row with realistic data ...
    quanta.add(row);
}

// builds indexes, must be called when new data is ingested
quanta.rebuild(); 

// 3. Query ads data
Query query = adEvents.newQuery()
    .and("region", "North America")
    .and("device", "Mobile", "Tablet")
    .gt("year", 2021) // Year > 2021
    .not("month", 5, 6)  // Exclude May & June data
    .lt("day", 15); // Include on when Day < 15

// 4. Process Query Results (Example)
query.forEach(row ->{
    // ... iterate through the results of the query ...
});
```

## Performance
The following example demonstrates the performance of Quanta with 20 million ad events on Apple M2:

```text
Populated 20,000,000 ad events in 29,685 ms

Finished searching records in: 4,724 ms

Ad events data for: Mobile
  Impressions: 168339958
  Clicks: 84016088
  Avg Bid Price: 2.5495461161066393
  # of Rows: 336,966

Ad events data for: Tablet
  Impressions: 167460993
  Clicks: 83637485
  Avg Bid Price: 2.549587425648991
  # of Rows: 335,330
```

## Comparable Solutions & How Quanta Differs
| Technology       | Key Features                                     | Differences from Quanta                                      |
|-----------------|-------------------------------------------------|--------------------------------------------------------------|
| **Apache Arrow** | Columnar memory format, zero-copy reads, efficient in-memory analytics | Arrow is **a file format**, while Quanta provides **indexing, filtering, and persistence**. |
| **Apache Druid** | Real-time OLAP, columnar storage, inverted indexing | Druid requires **distributed clusters**, while Quanta is **single-JVM, lightweight, and ultra-fast**. |
| **DuckDB**       | In-memory OLAP for analytical queries, columnar execution | DuckDB supports **SQL**, while Quanta is optimized for **direct memory access and low-latency filtering**. |
| **RocksDB**      | High-performance key-value store, LSM-tree-based indexing | RocksDB is **optimized for key-value lookups**, while Quanta is **better for analytical queries**. |
| **ClickHouse**   | Fast columnar database, OLAP workloads, compression | ClickHouse is **distributed**, while Quanta is a **single-instance, ultra-light alternative**. |
| **RedisBloom / RedisJSON** | In-memory caching, Bloom filters for indexing | Redis is a **key-value store**, while Quanta is **a structured, queryable data store**. |
| **MonetDB**      | Columnar storage, in-memory OLAP | MonetDB is **a full SQL database**, whereas Quanta is **schema-defined but query-optimized**. |

## What Quanta Doesn't Support (Yet)
While Quanta is highly optimized for speed and memory efficiency, there are certain limitations to be aware of.

1. Deleting Rows is Not Supported
* **Why?** Quanta is optimized for append-only writes, similar to how columnar databases work.
* **Workaround:** If you need to "delete" a row, consider marking it as inactive using a status column (e.g., deleted = true) and filtering on deleted = false. 
2. Filtering on Fact Columns is Not Supporte
* **Why?** Fact columns store raw numerical data without indexing, making direct filtering inefficient.
3. Partial Text Search is Not Supported
* **Why?** Quanta uses dictionary encoding for string storage, meaning it stores only exact matches.
* **Workaround:** Fetch all unique labels in a column and query for the specific ones you are interested in.

## License

Apache 2.0  

## Contributing

Contributions are welcome!  

## TODO
* Write test cases
* Write JavaDoc
* Publish to Maven Central
