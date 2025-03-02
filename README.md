# An Ultra-Fast, Memory-Efficient Data Engine for Analytical Workloads

Quanta is a high-performance analytical data engine designed for **millisecond query execution**, **minimal memory footprint**, and **lightning-fast ingestion**. Built for **single-node performance**, it eliminates the complexity of distributed systems while delivering **enterprise-grade speed** for analytical workloads.

## Key Features & Benefits

* **Blazing-Fast Queries** – Execute complex slice & dice queries on millions of records in milliseconds.
* **Efficient Data Ingestion:** Rapid loading & appending with optimized memory management.
* **Ultra-Low Memory Usage** – Efficient memory allocation & dictionary encoding.
* **Zero GC Overhead** – Direct memory management eliminates Java's garbage collection pauses.
* **Crash-Resilient & Persistent** – Memory-mapped files enable instant reloads & durability.
* **Real-Time & Batch Processing** – Supports incremental updates with low-latency reads.
* **Zero Third-Party Dependencies** – A pure **Java** implementation with no external libraries.

## How Quanta Works

Quanta delivers ultra-fast analytics by leveraging advanced memory management, columnar storage, and optimized indexing:

* **Raw Memory Allocation:** Manages memory outside the Java heap for optimal control.
* **Memory-Mapped Files:** Provides fast persistence, instant reloads, and efficient data access.
* **Dictionary-Based Compression:** Compresses repeated values for efficient string storage.
* **Bitset Indexing:** Enables ultra-fast query execution.

## Benchmark Scenarios

| Example Name        | # Rows (Millions)       | Populate Time    | Query Time     | Avg Column Size (MB) |
|---------------------|---------------|------------------|----------------|---------------------|
| [NYC Taxi](https://github.com/sagaripte/Quanta/blob/main/src/main/java/com/quanta/examples/NYCTaxiData.java)       | 20    | 47 secs              | 1.5 sec            | 25                 |
|        | 50    | 2.07 min               | 3.6 sec            | 60                 |
|         | 100   | 4.5 min              | 7.3 sec            | 115                 |
|                     | 150  | 6.5 min              | 10 sec            | 160                 |
| | | | | |
| [Ad Events](https://github.com/sagaripte/Quanta/blob/main/src/main/java/com/quanta/examples/AdEvents.java)       | 20    | 28 sec              | 1.1 sec            | 25                 |
|        | 50    | 1.23 min              | 2.9 sec            | 60                 |
|          | 100   | 2.5 min              | 5.8 sec            | 110                 |
|                     | 150   | 3.9 min              | 8.9 sec            | 160                 |

## Quick Start Example (Ad Event Analysis)

```java
// 1. Define the dataset schema
Quanta adEvents = new QuantaBuilder("AdEvents", "/data/ad_tracking")
    .addIntPrimaryKey("event_id")
    .addStringColumn("campaign", 30, IndexCardinality.TINY)
    .addDictionaryColumn("region", 10)
    .addDictionaryColumn("device", 10)
    .addIntColumn("year", IndexCardinality.TINY)
    .addIntColumn("month", IndexCardinality.TINY)
    .addIntColumn("day", IndexCardinality.TINY)
    .addIntMetric("impressions")
    .addIntMetric("clicks")
    .addMetric("bid_price")
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
