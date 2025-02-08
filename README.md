# An Ultra-Fast, Memory-Efficient Data Engine for Analytical Workloads

Quanta is a high-performance analytical data engine designed for **millisecond query execution**, **minimal memory footprint**, and **lightning-fast ingestion**. Built for **single-node performance**, it eliminates the complexity of distributed systems while delivering **enterprise-grade speed** for analytical workloads.

## Key Features & Benefits

* **Blazing-Fast Queries** – Execute complex queries on millions of records in milliseconds (<5ms / 20M rows).
* **Efficient Data Ingestion:** Rapid loading & appending with optimized memory management (<30ms / 20M records).
* **Ultra-Low Memory Usage** – Efficient memory allocation & dictionary encoding (~25MB/column for 20M rows).
* **Zero GC Overhead** – Direct memory management eliminates Java's garbage collection pauses.
* **Crash-Resilient & Persistent** – Memory-mapped files enable instant reloads & durability.
* **Real-Time & Batch Processing** – Supports incremental updates with low-latency reads.
* **Zero Third-Party Dependencies** – A pure **Java** implementation with no external libraries.

## How Quanta Works
Quanta bypasses Java's garbage collection by directly managing memory, minimizing footprint and eliminating GC pauses using:
* **Raw Memory Allocation:** Manages memory outside the Java heap for optimal control.
* **Memory-Mapped Files:** Provides fast persistence, instant reloads, and efficient data access.
* **Dictionary-Based Compression:** Compresses repeated values for efficient string storage.
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
    .lt("day", 15); // Include only when Day < 15

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

## Quanta vs. Other Solutions

| Technology       | Key Features                                 | How Quanta Differs                        |
|-----------------|---------------------------------------------|--------------------------------------------|
| **Apache Arrow**| Columnar memory format, zero-copy reads     | **Full indexing, filtering, persistence** |
| **Apache Druid**| Real-time OLAP, requires cluster setup      | **Single-node, no dependencies**         |
| **DuckDB**      | SQL-based in-memory OLAP engine            | **Direct memory access, low-latency filters** |
| **RocksDB**     | High-performance key-value store           | **Optimized for analytical queries**      |
| **ClickHouse**  | Fast OLAP DB, columnar storage, distributed | **Single-instance, ultra-light alternative** |


## What Quanta Doesn't Support (Yet)
While Quanta is highly optimized for speed and memory efficiency, there are certain limitations to be aware of.

* **No Row Deletions** – Workaround: Use a deleted = true flag.
* **No Filtering on Fact Columns** – Fact columns store raw values, no indexing.
* **No Partial Text Search** – Uses dictionary encoding for exact matches.

## License

Apache 2.0  

## Contributing

Contributions are welcome!  

## TODO
* Write test cases
* Write JavaDoc
* Publish to Maven Central
