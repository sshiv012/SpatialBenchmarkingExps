# SQL++ Queries and Execution Times

Source workbook: `Benchmark_Execution_Times.xlsx`  
All execution times are in seconds unless noted otherwise.

Related shapefile external dataset pushdown notes: `ShapefileTests.md`.

## Q1: Load Dataset from `localfs`

```sql
LOAD DATASET <dataset>
USING localfs
(
  ("path"=
    "ec01:///.../combined_part_1.json,
     ec02:///.../combined_part_2.json,
     ...
     ec12:///.../combined_part_12.json"
  ),
  ("format"="adm")
);
```

| Dataset | USA | Western Hemisphere | Whole World |
|---|---:|---:|---:|
| Buildings | 41.41 | 453.14 | 65.36 |
| All Nodes | 286.88 | 683.54 | 427.85 |

## Q2: Create R-tree Index

```sql
CREATE INDEX geom_idx ON <dataset>(geometryAttribute) TYPE RTREE;
```

| Dataset | USA | Western Hemisphere | Whole World |
|---|---:|---:|---:|
| Buildings | 16.77 | 294.48 | 476.12 |
| All Nodes | 844.87 | 6741.93 | 9286.38 |

## Q3: Spatial Join (Default PBSM, 100x100 Grid)

```sql
SELECT COUNT(*)
FROM all_nodes_dataset a, buildings_dataset b
WHERE st_contains(b.g, a.g);
```

| Configuration | USA | Western Hemisphere | Whole World |
|---|---:|---:|---:|
| PBSM default 100x100 grid (JTS) | 3934.07 | 21793.66 | 35896.05 |
Note will update this after increasing parallelism conf. Once again.

## Spatial Join in Sedona, 12 Nodes
### Sedona 12-Node Execution Details

Source files:
- `SedonaJoinTestAPR2026.scala`
- `../query_log_show_collect_apr_17_26.txt`

The Sedona driver registers Sedona SQL functions, reads buildings and all-node datasets from HDFS, converts WKT strings into geometry values, filters buildings to polygons, and runs the same containment-count query:

```sql
SELECT COUNT(*) AS c
FROM all_nodes_dataset a, buildings_dataset b
WHERE ST_Contains(b.g, a.g)
```

Input paths used by the Scala driver:

| Dataset | HDFS Path |
|---|---|
| Buildings | `hdfs:///user/asevi006/osm2105_buildings_adm/` |
| All nodes | `hdfs:///user/asevi006/osm2105_all_nodes_adm/` |

Spark runtime configuration from the log:

| Setting | Value |
|---|---:|
| Spark master | `spark://ec-hn.cs.ucr.edu:7077` |
| Cluster nodes | 12 |
| Cores per node | 12 |
| Total executor cores / task slots | 144 |
| RAM per node | 64 GB |
| Spark executor cores | 4 |
| Spark executor memory | 16g |
| Spark driver memory | 16g |
| Expected Spark executor JVMs with 4 cores each | 36 |
| Observed worker executor JVM entries in log | 36 |
| Theoretical one-core executors if split 1 core each | 144 |
| Physical RAM per one-core executor slot | 5.33 GB |

Execution time:

| Action | Time (seconds) | Time (minutes) | Spark job IDs |
|---|---:|---:|---|
| `show(truncate = false)` | 1686.83 | 28.11 | `4,5,6,7` |
| `collect()` | 3011.00 | 50.18 | `8,9,10,11` |
| Total action time | 4697.83 | 78.30 | `4,5,6,7,8,9,10,11` |

Total action time is approximately `1.31 hours`.

| Metric | Value |
|---|---:|
| Containment pairs counted | 12,210,542 |
| Rows returned by `collect()` | 1 |

Spark planned the query as a Sedona spatial range join:

```text
RangeJoin g#46: geometry, g#25: geometry, WITHIN
```

## Q4: Spatial Join with Spatial-Partitioning Hint

```sql
SELECT COUNT(*)
FROM all_nodes_dataset a, buildings_dataset b
WHERE /*+ spatial-partitioning(-180.0, -89.0, 180.0, 90.0, 1200000, 1200000) */
      st_contains(b.g, a.g);
```

| Configuration | USA | Western Hemisphere | Whole World |
|---|---:|---:|---:|
| PBSM optimized 1.2M x 1.2M grid (JTS) | tbf | tbf | 1102.95 |

### Optimization History

The optimized spatial join time was reduced through the following configuration and execution changes:

| # | Change | Time Before -> After | Saving | Notes |
|---:|---|---:|---:|---|
| 1 | `compiler.parallelism` `12` -> `96` | ~3452 s -> 1512 s | ~1940 s | Default was 1 partition per NC, so only 12 of 144 cluster cores were in use. Set to `storage.partitions.count` (`8`) x 12 NCs = 96. |
| 2 | Shut down HDFS DataNode, NodeManager, and NameNode | 1512 s -> 1203 s | ~310 s | DataNode periodically ran `du -sk` over 6 TB block files, thrashing the kernel page cache. |
| 3 | Swap `FROM`-clause order so the smaller dataset is on the left | At p=12: 3910 s -> 3452 s | ~460 s | `SpatialJoiner.java:92-107` unconditionally spills the BUILD side to a disk runfile. Smaller build side means about 7x less HDD I/O for spill. Verify in the plan with `SPATIAL_JOIN [keys-left] [keys-right]`; left is BUILD. |
| 4 | `compiler.joinmemory` `1GB` -> `2GB` at p=96 | 1193 s -> 1132 s | ~61 s | Larger PBSM probe buffer reduces build-runfile re-scans. Beyond about 2 GB at p=96, per-partition probe data (~885 MB) fits comfortably, so further increases did not help. |
| 5 | `compiler.sortmemory` `64MB` -> `128MB` | 1113 s -> 1102 s | ~11 s | Improvement is small but reproducible. Sort is about 30% of CPU, but per-partition spill volume of 1.55 GB exceeds what can fit in heap at p=96 without OOM. |

### Profiling Breakdown

Operator-level profiling showed that the remaining optimized runtime was dominated by the plane-sweep join, external sort, and the `st_contains` refine filter:

| Operator | CPU-sec | Share | Notes |
|---|---:|---:|---|
| PSJ `JoinProbe` | 9,709 | 21% | Plane-sweep join itself |
| Sort, run merge | 9,097 | 20% | Merging external sort runs; 297 GB total processed |
| `stream-select` (`st_contains`) | 6,456 | 14% | Refine filter; drops 98% of 531M MBR-passing candidates |
| PSJ `JoinBuild` | 5,179 | 11% | Build-side runfile write |
| Sort, run generation | 4,817 | 10% | Initial sort runs spilled to disk |
| Index Search (`DATASCAN`) | 4,057 | 9% | Cold reads from row-format dataset |
| `assign` (`st-mbr`, `getField`) | 3,974 | 9% | MBR computation |
| `unnest` (`SpatialTileDescriptor`) | 2,215 | 5% | Tile replication |

Sort accounts for about 30% of CPU when run generation and run merge are combined. Sort spill volume per NC was about 24 GB written plus 24 GB read, or about 48 GB of HDD throughput per NC just for sort. The PBSM build runfile is currently unavoidable because `SpatialJoiner.java:92-95` opens the runfile in the constructor unconditionally, with no in-memory fast path.

### Best Recorded Optimized Run

Best recorded Q4 time: `1102.95 s` execution time (`1102.95 s` elapsed time). The run used `96` compiler parallelism, `2GB` join memory, `128MB` sort memory, and a JVM heap of `32GB` per worker.

```bash
curl -sS http://localhost:19002/query/service \
  --data-urlencode 'statement=SET `compiler.parallelism` "96"; SET `compiler.joinmemory` "2GB"; SET `compiler.sortmemory` "128MB"; USE test; SELECT COUNT(*) FROM buildings_dataset b, all_nodes_dataset a WHERE /*+ spatial-partitioning(-180.0, -89.0, 180.0, 90.0, 1200000, 1200000) */ st_contains(b.g, a.g);' \
  --data "pretty=true" \
  --data "client_context_id=p96-j2g-s128m-back-to-31g"
```

| Setting / Metric | Value |
|---|---:|
| `compiler.parallelism` | 96 |
| `compiler.joinmemory` | 2GB |
| `compiler.sortmemory` | 128MB |
| Worker JVM heap | 32GB per worker |
| Query result | 12,210,542 |
| Elapsed time | 1102.95 s |
| Execution time | 1102.95 s |
| Compile time | 46.26 ms |
| Queue wait time | 1 ms |
| Processed objects | 2,795,608,586 |
| Buffer cache hit ratio | 0.00% |
| Buffer cache page reads | 3,104,497 |

### Sedona Comparison

The Sedona run executed both `show(truncate = false)` and `collect()` on the same aggregate DataFrame. Since the DataFrame was not cached and the query returns a single aggregate row, both actions trigger the same query computation. The `show()` action is the better primary comparison here because it was the first action executed and therefore reflects the initial Sedona query execution.

Using Sedona `show()` as the first-action baseline:

| System / Run | Comparable Time | Notes |
|---|---:|---|
| Sedona, `show(truncate = false)` | 1686.83 s | First Spark action from the log |
| AsterixDB, best Q4 optimized run | 1102.95 s | `compiler.parallelism=96`, `compiler.joinmemory=2GB`, `compiler.sortmemory=128MB`, 32GB JVM heap per worker |
| Speedup | 1.53x | AsterixDB best optimized run is about 1.53x faster than Sedona's first action |

The later Sedona `collect()` action took `3011.00 s`, but it repeats the query and should not be added to `show()` for the primary benchmark. Adding `show + collect` gives `4697.83 s` and would overstate the comparison because it counts two separate Spark actions.

## Q5: Force Nested Loop Join in AsterixDB

```sql
SELECT COUNT(*)
FROM all_nodes_dataset a, buildings_dataset b
WHERE st_contains(b.g, a.g)=true;
```

| System | Time | Storage | Status |
|---|---:|---|---|
| AsterixDB Nested Loop Join | 162000.00 | AsterixDB | Aborted after about 45 hours |

## Q6: Spatial Partitioning Sweep Template

```sql
SELECT COUNT(*)
FROM all_nodes_dataset a, buildings_dataset b
WHERE /*+ spatial-partitioning(-180.0, -89.0, 180.0, 90.0, x, x) */
      st_contains(b.g, a.g);
```

| Grid Size | Whole World Time | Notes |
|---:|---:|---|
| 12000.0 | 5876.69 |  |
| 240007.0 | 3311.28 |  |
| 1200109.0 | 2962.80 | Optimal region |
| 2400109.0 | 3205.26 |  |
| 4800113.0 | 10000.00 | Performance degrades |

Note: Can increase parallelism for these and re-run

## Q7: Spatial Filtering (Region Polygon) + Spatial Join

```sql
SELECT COUNT(*)
FROM buildings_dataset b, all_nodes_dataset a
WHERE ST_Contains(ST_GeomFromGeoJSON(<region_polygon>), b.g)
  AND ST_Contains(b.g, a.g);
```

| Filter Region | USA Broadcast | USA Indexed | Western Hemisphere Broadcast | Western Hemisphere Indexed | Whole World Broadcast | Whole World Indexed |
|---|---:|---:|---:|---:|---:|---:|
| Riverside (159 records) | 223.11 | 0.87 | 818.95 | 0.86 | 972.90 | 0.92 |
| LA County (49,175 records) | 231.05 | 175.80 | 823.34 | 176.57 | 996.70 | 212.30 |
| California (389,531 records) | 285.80 | 1530.46 | 865.10 | 1425.00 | 1029.53 | 1719.97 |

## Q8: Range-Count Query (Indexed)

```sql
SELECT COUNT(*) AS c
FROM nodes_dataset_clean n
WHERE ST_Intersects(n.g, ST_Make_Envelope(xmin, ymin, xmax, ymax, 4326));
```

| Region | Records | Selectivity (%) | Indexed Time |
|---|---:|---:|---:|
| SF (0.02 deg) | 938 | <0.01 | 1.21 |
| SF (0.04 deg) | 3833 | <0.01 | 0.06 |
| Bay Area | 67353 | 0.07 | 1.42 |
| NorCal | 281798 | 0.28 | 4.00 |
| West USA | 3040377 | 3.06 | 22.00 |
| Western Hemisphere | 31819657 | 32.03 | 24.40 |
| Eastern Hemisphere | 67518605 | 67.97 | 55.58 |
| World | 99338248 | 100.00 | 95.35 |

## Q9: Force Full Scan

```sql
SELECT COUNT(*) AS c
FROM nodes_dataset_clean n
WHERE /*+ skip-index */
      ST_Intersects(n.g, ST_Make_Envelope(xmin, ymin, xmax, ymax, 4326));
```

| Region | Records | Selectivity (%) | Scan Time |
|---|---:|---:|---:|
| SF (0.02 deg) | 938 | <0.01 | 43.99 |
| SF (0.04 deg) | 3833 | <0.01 | 27.64 |
| Bay Area | 67353 | 0.07 | 26.90 |
| NorCal | 281798 | 0.28 | 23.79 |
| West USA | 3040377 | 3.06 | 25.03 |
| Western Hemisphere | 31819657 | 32.03 | 30.00 |
| Eastern Hemisphere | 67518605 | 67.97 | 25.04 |
| World | 99338248 | 100.00 | 28.90 |

## Q10: Circular Range Query (Golden Gate Bridge, r = 0.1)

```sql
SELECT COUNT(*)
FROM <dataset>
WHERE ST_Distance(ST_Point(-122.4783, 37.8199), g) < 0.1;
```

| Query | Sedona | AsterixDB |
|---|---:|---:|
| Range, no index, Buildings near Golden Gate | 45.73 | 65.73 | 
| Range, no index, All Nodes near Golden Gate | 302.59 | 606.61 |

## Q11: Spatial Aggregation (Average Building Area)

```sql
SELECT AVG(st_area(b.g)) AS average_builiding_area
FROM buildings_dataset b;
```

| System | Execution Time |
|---|---:|
| Sedona | 44.20 | 
| AsterixDB | 45.82 |

## Q12: Spatial Aggregation (Minimum X)

```sql
SELECT MIN(st_x(a.g)) AS min_x
FROM all_nodes_dataset a;
```
| System | Execution Time |
|---|---:|
| Sedona | 293.83 | 
| AsterixDB | 534.60 |

## Horizontal Scale-Out: USA Subset

| Operation | 4 Nodes | 8 Nodes | 12 Nodes | Speedup 4 -> 12 |
|---|---:|---:|---:|---:|
| Data Loading - All Nodes | 4,150.64 | 2,434.83 | 1,286.88 | 3.2x |
| Data Loading - Buildings | 117.20 | 74.75 | 41.40 | 2.8x |
| Spatial Join - PBSM | 2,117.88 | 1,033.29 | 503.05 | 4.2x |
| Range All Nodes (Golden Gate) | 495.23 | 258.75 | 138.48 | 3.6x |
| Range Buildings (Golden Gate) | 14.93 | 9.91 | 4.39 | 3.4x |
| Spatial Agg - Avg Building | 8.93 | 4.40 | 3.71 | 2.4x |
| Spatial Agg - Min X All Nodes | 370.94 | 196.87 | 120.61 | 3.1x |
| Index Creation - All Nodes | 2,872.60 | 1,382.94 | 844.67 | 3.4x |
| Index Creation - Buildings | 42.23 | 20.69 | 16.87 | 2.5x |

# Real-World Analytical Use Cases
## Q13: CA-Wide Containment Join (Buildings Join Nodes) with BBox Filters

```sql
SELECT COUNT(*) AS total_containment_pairs
FROM buildings_dataset_clean cb
JOIN nodes_dataset_clean cn
ON ST_Contains(cb.g, cn.g)
WHERE ST_Intersects(cb.g, st_make_envelope(-124.48, 32.53, -114.13, 42.01, 4326))
AND ST_Intersects(cn.g, st_make_envelope(-124.48, 32.53, -114.13, 42.01, 4326));
```

| System / Index Configuration | Time |
|---|---:|
| Sedona | 147.25 |
| AsterixDB Full Index | 27.02 |
| AsterixDB No Index | 108.45 |

## Q14: Self Spatial Join (Overlapping Building Pairs) in SF Window

```sql
SELECT COUNT(*) AS overlapping_pairs
FROM buildings_dataset_clean cb1
JOIN buildings_dataset_clean cb2
ON ST_Intersects(cb1.g, cb2.g)
WHERE cb1.name IS NOT NULL AND cb2.name IS NOT NULL
AND cb1.name < cb2.name
AND ST_Intersects(cb1.g, st_make_envelope(-122.52, 37.70, -122.35, 37.82, 4326))
AND ST_Intersects(cb2.g, st_make_envelope(-122.52, 37.70, -122.35, 37.82, 4326));
```

| System / Index Configuration | Time |
|---|---:|
| Sedona | 99.50 |
| AsterixDB Full Index | 8.10 |
| AsterixDB No Index | 41.30 |

## Q15: Multi-Index Selective Join + Aggregation

```sql
SELECT cb.name AS building_name,
cb.building AS building_type,
cb.addr_city,
array_agg(cn.name) AS contained_poi_names
FROM buildings_dataset_clean cb
JOIN nodes_dataset_clean cn
ON ST_Contains(cb.g, cn.g)
WHERE cb.name IS NOT NULL
AND cb.amenity = "university"
AND cn.name IS NOT NULL
AND ST_Intersects(cb.g, st_make_envelope(-118.5, 34.0, -118.1, 34.1, 4326))
GROUP BY cb.name, cb.building, cb.addr_city
LIMIT 10;
```

| System / Index Configuration | Time |
|---|---:|
| Sedona | 92.06 |
| AsterixDB All Index | 0.21 |
| AsterixDB BTree Only | 16.80 |
| AsterixDB RTree Only | 0.30 |
| AsterixDB No Index | 76.67 |

## Q16: Dual-Filter Containment Join (Schools Contain Schools) in CA Window

```sql
SELECT COUNT(*) AS matches
FROM buildings_dataset_clean cb
JOIN nodes_dataset_clean cn
ON ST_Contains(cb.g, cn.g)
WHERE cb.amenity = "school" AND cn.amenity = "school"
AND ST_Intersects(cb.g, st_make_envelope(-124.48, 32.53, -114.13, 42.01, 4326));
```

| System / Index Configuration | Time |
|---|---:|
| Sedona | 162.90 |
| AsterixDB Full Index | 1.60 |
| AsterixDB BTree Only | 10.28 |
| AsterixDB RTree Only | 3.30 |
| AsterixDB No Index | 65.80 |

## OGC (JTS) vs Legacy Geometry in AsterixDB

### SF Window Range Query (`ST_Intersects`)

| Geometry | Indexed Time | No Index Time | Index Speedup |
|---|---:|---:|---:|
| OGC (JTS) | 1.21 | 43.99 | 36.30x |
| Legacy | 21.20 | 225.54 | 10.60x |

### Aggregation Queries (Scan-Based)

| Query | OGC Time | Legacy Time | Notes |
|---|---:|---:|---|
| MAX Area Aggregation | 71.78 | 57.75 | Legacy faster because geometry representation is simpler |
| MIN X Aggregation | 31.78 | 21.26 | Legacy faster because geometry representation is simpler |