# Design Decisions and Trade-offs

## Delta-rs vs PySpark for Silver

**Decision**: Use `deltalake` (delta-rs) instead of PySpark + Delta Lake.

**Rationale**: Lighter for local development; no JVM/Spark cluster. Supports ACID, time travel, schema evolution. For cloud migration, Spark + Delta can be added later if needed.

## Parquet vs Delta in Gold

**Decision**: Parquet with overwrite per run (no Delta in Gold for this pipeline).

**Rationale**: Gold is a simple aggregated view recomputed each run. We do not use Delta here because the pipeline is intentionally simple: one aggregated Parquet file per run, no ACID, time travel, or merge semantics needed. Parquet is lighter (no transaction log) and sufficient for read-only analytical queries. For more complex or high-volume Gold layers (e.g. incremental aggregates, multiple consumers, or very large outputs), using Delta (or similar) in Gold would be a reasonable option—e.g. for ACID, partitioning, or merge/append patterns.

## Partitioning: country + state_province

**Decision**: Partition Silver by `country` and `state_province`.

**Rationale**: Location-based partitioning as per requirements. Adding `city` would increase partition count significantly without clear benefit for the current use case.

## Daily Schedule

**Decision**: DAG runs daily (`@daily`).

**Rationale**: Brewery data (names, locations, types) changes infrequently. Daily balances freshness with resource usage and API load. Hourly runs would not justify the added cost.

## Bronze: Atomic Write

**Decision**: Write to temp file first; rename to final path only on success.

**Rationale**: Guarantees no partial or corrupted data if the run fails during execution (e.g. mid-write, validation failure).

## Required Fields in Bronze Validation

**Decision**: Require `id`, `name`, `brewery_type`, `city`, `state_province`, `country`, `postal_code`, `state` (non-null per API docs).

**Rationale**: Ensures data integrity before persistence. API documentation states these fields do not accept null.

## Task Communication: File Path vs XCom

**Decision**: Fetch task writes to staging file, passes path to load_bronze; avoid large XCom payloads.

**Rationale**: ~9k records can exceed XCom size limits or slow task serialization. File-based handoff is more scalable.

## LocalExecutor

**Decision**: Use LocalExecutor for Docker setup.

**Rationale**: Simpler than CeleryExecutor; no Redis. Suitable for local and small-scale deployments.

---

## Silver “merge” is in-memory only; Delta write is still overwrite

**What the code actually does**

When `merge_with_existing=True`:

1. **Read**: Load the **entire** existing Silver table into memory (`DeltaTable(...).to_pandas()`).
2. **Merge in memory**: Concat that DataFrame with the new Bronze-derived DataFrame, then `drop_duplicates(subset=["id"], keep="last")` using `ingested_at` so the latest run wins. So the “merge” is **only at the pandas/DataFrame level**; the full Silver table is in memory.
3. **Write**: Call `write_deltalake(..., mode="overwrite")`. That **replaces the whole Delta table** with the merged result. There is **no Delta-level upsert**: no `MERGE INTO`, no conditional update/insert, no appending only changed partitions. The table is overwritten in one shot.

**Implications**

- **Memory**: Every run that merges loads **full Silver + new Bronze** into memory. So we did **not** get a true “streaming” or “incremental” merge; we only avoided re-reading all historical Bronze (we only read this run’s Bronze).
- **Scalability**: For large Silver tables, this pattern does not scale: memory and write cost grow with total table size each run.
- **Real upsert** would mean using Delta’s **merge** (e.g. Spark `DeltaTable.forPath(...).alias("t").merge(newData.alias("s"), "t.id = s.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()`), or delta-rs equivalent if available, so that only changed/new rows are written and the rest of the table is left as-is. The Python `deltalake` (delta-rs) library we use does **not** expose a high-level merge API; merge would require Spark SQL, Delta Standalone, or another engine that supports `MERGE INTO`.

**Summary**

| Aspect              | Current behaviour                                      |
|---------------------|--------------------------------------------------------|
| Where “merge” happens | In memory (pandas concat + dedupe)                    |
| What gets written   | Full merged result via `write_deltalake(..., mode="overwrite")` |
| Real Delta upsert?  | No — whole table is overwritten                       |
| Memory              | Full Silver + new Bronze in memory when merging       |

So: we do a **logical** merge (correct semantics: latest row per `id` wins) but **not** a storage-level upsert. For production at scale, a real merge (e.g. Spark + Delta MERGE) or append-only Silver with a dedupe view would be the next step.
