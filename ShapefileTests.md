# Chicago External Dataset Pushdown Evaluation

This document captures the DDL, benchmark queries, execution times, and logical plans for evaluating **projection pushdown** and **filter pushdown** on the `ChicagoSHP` shapefile-backed external dataset, with a comparison against the `ChicagoADM` JSON-backed external dataset. I added hints and modified some code to get this working. 


## Setup

```sql
-- DDL
DROP DATAVERSE bench IF EXISTS;
CREATE DATAVERSE bench;
USE bench;

-- Open type works for both SHP (DBF field names, truncated) and ADM (full JSON names)
CREATE TYPE CrimesType AS { };

-- Shapefile external dataset (6 shapefiles in directory)
CREATE EXTERNAL DATASET ChicagoSHP(CrimesType) USING hdfs (
  ("hdfs"        = "file:///"),
  ("path"        = "/Users/suryaacharan/Downloads/shapefile-test/CC/Chicago_Crimes/"),
  ("input-format"= "shapefile")
);

-- ADM (JSON lines) external dataset
CREATE EXTERNAL DATASET ChicagoADM(CrimesType) USING localfs (
  ("path"         = "localhost:///Users/suryaacharan/Downloads/shapefile-test/CC/Chicago_Crimes.json"),
  ("input-format" = "text-input-format"),
  ("format"       = "json")
);
```

## Query 1 — Combined Aggregate

### Baseline

```sql
SELECT MIN(st_x(g)) AS min_x, MIN(Date) AS min_date FROM ChicagoSHP;
```

**Execution Time:** `19.14 s`  
**Result:** `{"min_x":-91.686565684,"min_date":"01/01/2001 01:00:00 AM"}`

#### Plan

```text
distribute result [$$45]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"min_x": $$47, "min_date": $$48}
        └── AGGREGATE global min($$50), min($$51)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE local min($$38), min($$43)
                    └── ASSIGN $$38 = st-x(g), $$43 = Date
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                project ({g:any,Date:any})
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$45] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$45] <- [{"min_x": $$47, "min_date": $$48}] project: [$$45] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$47, $$48] <- [agg-global-sql-min($$50), agg-global-sql-min($$51)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$50, $$51] <- [agg-local-sql-min($$38), agg-local-sql-min($$43)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            assign [$$38, $$43] <- [st-x($$ChicagoSHP.getField("g")), $$ChicagoSHP.getField("Date")] project: [$$38, $$43] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ASSIGN  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP project ({g:any,Date:any}) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

### Skip Projection Pushdown

```sql
USE bench;
SELECT MIN(st_x(g)) AS min_x, MIN(Date) AS min_date
FROM /*+ skip-projection-pushdown */ ChicagoSHP;
```

**Execution Time:** `21.04 s`  
**Result:** `{"min_x":-91.686565684,"min_date":"01/01/2001 01:00:00 AM"}`

#### Plan

```text
distribute result [$$45]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"min_x": $$47, "min_date": $$48}
        └── AGGREGATE global min($$50), min($$51)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE local min($$38), min($$43)
                    └── ASSIGN $$38 = st-x(g), $$43 = Date
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                (full tuple scan; no pushed projection)
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$45] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$45] <- [{"min_x": $$47, "min_date": $$48}] project: [$$45] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$47, $$48] <- [agg-global-sql-min($$50), agg-global-sql-min($$51)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$50, $$51] <- [agg-local-sql-min($$38), agg-local-sql-min($$43)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            assign [$$38, $$43] <- [st-x($$ChicagoSHP.getField("g")), $$ChicagoSHP.getField("Date")] project: [$$38, $$43] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ASSIGN  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

## Query 2 — Spatial Aggregate Only

### Baseline

```sql
SELECT MIN(st_x(g)) AS min_x FROM ChicagoSHP;
```

**Execution Time:** `5.522 s`  
**Result:** `{"min_x": -91.686565684}`

#### Plan

```text
distribute result [$$36]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"min_x": $$37}
        └── AGGREGATE global min($$40)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE local min($$34)
                    └── ASSIGN $$34 = st-x(g)
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                project ({g:any})
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$36] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$36] <- [{"min_x": $$37}] project: [$$36] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$37] <- [agg-global-sql-min($$40)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$40] <- [agg-local-sql-min($$34)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            assign [$$34] <- [st-x($$ChicagoSHP.getField("g"))] project: [$$34] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ASSIGN  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP project ({g:any}) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

### Skip Projection Pushdown

```sql
SELECT MIN(st_x(g)) AS min_x FROM /*+ skip-projection-pushdown */ ChicagoSHP;
```

**Execution Time:** `18.935 s`  
**Result:** `{"min_x": -91.686565684}`

#### Plan

```text
distribute result [$$36]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"min_x": $$37}
        └── AGGREGATE global min($$40)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE local min($$34)
                    └── ASSIGN $$34 = st-x(g)
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                (full tuple scan; no pushed projection)
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$36] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$36] <- [{"min_x": $$37}] project: [$$36] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$37] <- [agg-global-sql-min($$40)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$40] <- [agg-local-sql-min($$34)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            assign [$$34] <- [st-x($$ChicagoSHP.getField("g"))] project: [$$34] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ASSIGN  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

## Query 3 — Non-Spatial Aggregate Only

### Baseline

```sql
SELECT MIN(Date) AS min_date FROM ChicagoSHP;
```

**Execution Time:** `14.05 s`  
**Result:** `{"min_date":"01/01/2001 01:00:00 AM"}`

#### Plan

```text
distribute result [$$35]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"min_date": $$36}
        └── AGGREGATE global min($$38)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE local min($$33)
                    └── ASSIGN $$33 = Date
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                project ({Date:any})
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$35] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$35] <- [{"min_date": $$36}] project: [$$35] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$36] <- [agg-global-sql-min($$38)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$38] <- [agg-local-sql-min($$33)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            assign [$$33] <- [$$ChicagoSHP.getField("Date")] project: [$$33] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ASSIGN  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP project ({Date:any}) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

### Skip Projection Pushdown

```sql
SELECT MIN(Date) AS min_date FROM /*+ skip-projection-pushdown */ ChicagoSHP;
```

**Execution Time:** `18.15 s`  
**Result:** `{"min_date": "01/01/2001 01:00:00 AM"}`

#### Plan

```text
distribute result [$$35]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"min_date": $$36}
        └── AGGREGATE global min($$38)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE local min($$33)
                    └── ASSIGN $$33 = Date
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                (full tuple scan; no pushed projection)
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$35] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$35] <- [{"min_date": $$36}] project: [$$35] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$36] <- [agg-global-sql-min($$38)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$38] <- [agg-local-sql-min($$33)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            assign [$$33] <- [$$ChicagoSHP.getField("Date")] project: [$$33] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ASSIGN  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

## Query 4 — Count All Rows

### Baseline

```sql
SELECT COUNT(*) AS total FROM ChicagoSHP;
```

**Execution Time:** `3.82 s`  
**Result:** `{"total":7078879}`

#### Plan

```text
distribute result [$$33]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"total": $$34}
        └── AGGREGATE sum($$35)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE count(1)
                    └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                        └── DATASOURCE_SCAN bench.ChicagoSHP
                            project ({})
                            └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$33] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$33] <- [{"total": $$34}] project: [$$33] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$34] <- [agg-sql-sum($$35)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$35] <- [agg-sql-count(1)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
              data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP project ({}) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- DATASOURCE_SCAN  |PARTITIONED|
                exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                  empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

### Skip Projection Pushdown

```sql
SELECT COUNT(*) AS total FROM /*+ skip-projection-pushdown */ ChicagoSHP;
```

**Execution Time:** `18.3 s`  
**Result:** `{"total":7078879}`

#### Plan

```text
distribute result [$$33]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"total": $$34}
        └── AGGREGATE sum($$35)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE count(1)
                    └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                        └── DATASOURCE_SCAN bench.ChicagoSHP
                            (full tuple scan; no pushed projection)
                            └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$33] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$33] <- [{"total": $$34}] project: [$$33] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$34] <- [agg-sql-sum($$35)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$35] <- [agg-sql-count(1)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
              data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- DATASOURCE_SCAN  |PARTITIONED|
                exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                  empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

## Query 5 — Spatial Filtered Count (Projection + Filter Pushdown)

### Baseline

```sql
SELECT COUNT(*)
FROM ChicagoSHP
WHERE st_within(g, st_make_envelope(-87.631, 41.798, -87.621, 41.818, 4326));
```

**Execution Time:** `4.08 s`  
**Result:** `{"$1":35465}`

#### Plan

```text
distribute result [$$39]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"$1": $$40}
        └── AGGREGATE sum($$42)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE count(1)
                    └── STREAM_SELECT st-within(g, envelope)
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                project ({g:any})
                                mbr-filter: [-87.631,41.798,-87.621,41.818]
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$39] <- [{"$1": $$40}] project: [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$40] <- [agg-sql-sum($$42)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$42] <- [agg-sql-count(1)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            select (st-within($$ChicagoSHP.getField("g"), POLYGON ((-87.631 41.798, -87.631 41.818, -87.621 41.818, -87.621 41.798, -87.631 41.798)))) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- STREAM_SELECT  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP project ({g:any}) mbr-filter: [-87.631,41.798,-87.621,41.818] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

### Skip Filter Pushdown

```sql
SELECT COUNT(*)
FROM /*+ skip-filter-pushdown */ ChicagoSHP
WHERE st_within(g, st_make_envelope(-87.631, 41.798, -87.621, 41.818, 4326));
```

**Execution Time:** `6.101 s`  
**Result:** `{"$1":35465}`

#### Plan

```text
distribute result [$$39]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"$1": $$40}
        └── AGGREGATE sum($$42)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE count(1)
                    └── STREAM_SELECT st-within(g, envelope)
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                project ({g:any})
                                (no pushed mbr-filter)
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$39] <- [{"$1": $$40}] project: [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$40] <- [agg-sql-sum($$42)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$42] <- [agg-sql-count(1)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            select (st-within($$ChicagoSHP.getField("g"), POLYGON ((-87.631 41.798, -87.631 41.818, -87.621 41.818, -87.621 41.798, -87.631 41.798)))) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- STREAM_SELECT  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP project ({g:any}) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

### Skip Projection Pushdown

```sql
SELECT COUNT(*)
FROM /*+ skip-projection-pushdown */ ChicagoSHP
WHERE st_within(g, st_make_envelope(-87.631, 41.798, -87.621, 41.818, 4326));
```

**Execution Time:** `12.93 s`  
**Result:** `{"$1":35465}`

#### Plan

```text
distribute result [$$39]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"$1": $$40}
        └── AGGREGATE sum($$42)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE count(1)
                    └── STREAM_SELECT st-within(g, envelope)
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                mbr-filter: [-87.631,41.798,-87.621,41.818]
                                (full tuple scan; no pushed projection)
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$39] <- [{"$1": $$40}] project: [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$40] <- [agg-sql-sum($$42)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$42] <- [agg-sql-count(1)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            select (st-within($$ChicagoSHP.getField("g"), POLYGON ((-87.631 41.798, -87.631 41.818, -87.621 41.818, -87.621 41.798, -87.631 41.798)))) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- STREAM_SELECT  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP mbr-filter: [-87.631,41.798,-87.621,41.818] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

### Skip Both Projection and Filter Pushdown

```sql
SELECT COUNT(*)
FROM /*+ skip-projection-pushdown*/ /*+ skip-filter-pushdown*/ ChicagoSHP
WHERE st_within(g, st_make_envelope(-87.631, 41.798, -87.621, 41.818, 4326));
```

**Execution Time:** `20.29 s`  
**Result:** `{"$1":35465}`

#### Plan

```text
distribute result [$$39]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"$1": $$40}
        └── AGGREGATE sum($$42)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE count(1)
                    └── STREAM_SELECT st-within(g, envelope)
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoSHP
                                (full tuple scan; no pushed projection; no mbr-filter)
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$39] <- [{"$1": $$40}] project: [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$40] <- [agg-sql-sum($$42)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$42] <- [agg-sql-count(1)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            select (st-within($$ChicagoSHP.getField("g"), POLYGON ((-87.631 41.798, -87.631 41.818, -87.621 41.818, -87.621 41.798, -87.631 41.798)))) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- STREAM_SELECT  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoSHP] <- bench.ChicagoSHP [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

## Query 5 — ADM Comparison

```sql
SELECT COUNT(*) AS cnt
FROM ChicagoADM
WHERE st_within(st_geom_from_text(g), st_make_envelope(-87.631, 41.798, -87.621, 41.818, 4326));
```

**Execution Time:** `35.96 s`  
**Result:** `{"$1":35465}`

#### Plan

```text
distribute result [$$39]
└── ONE_TO_ONE_EXCHANGE | UNPARTITIONED
    └── ASSIGN {"cnt": $$40}
        └── AGGREGATE sum($$42)
            └── RANDOM_MERGE_EXCHANGE | PARTITIONED
                └── AGGREGATE count(1)
                    └── STREAM_SELECT st-within(st-geom-from-text(g), envelope)
                        └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                            └── DATASOURCE_SCAN bench.ChicagoADM
                                (no pushed spatial filter / no shapefile-native pruning)
                                └── ONE_TO_ONE_EXCHANGE | PARTITIONED
                                    └── EMPTY_TUPLE_SOURCE
```

<details>
<summary>Copyable raw plan</summary>

```text
distribute result [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
-- DISTRIBUTE_RESULT  |UNPARTITIONED|
  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
  -- ONE_TO_ONE_EXCHANGE  |UNPARTITIONED|
    assign [$$39] <- [{"cnt": $$40}] project: [$$39] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
    -- ASSIGN  |UNPARTITIONED|
      aggregate [$$40] <- [agg-sql-sum($$42)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
      -- AGGREGATE  |UNPARTITIONED|
        exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
        -- RANDOM_MERGE_EXCHANGE  |PARTITIONED|
          aggregate [$$42] <- [agg-sql-count(1)] [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
          -- AGGREGATE  |PARTITIONED|
            select (st-within(st-geom-from-text($$ChicagoADM.getField("g")), POLYGON ((-87.631 41.798, -87.631 41.818, -87.621 41.818, -87.621 41.798, -87.631 41.798)))) [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
            -- STREAM_SELECT  |PARTITIONED|
              exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
              -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                data-scan []<-[$$ChicagoADM] <- bench.ChicagoADM [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                -- DATASOURCE_SCAN  |PARTITIONED|
                  exchange [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                  -- ONE_TO_ONE_EXCHANGE  |PARTITIONED|
                    empty-tuple-source [cardinality: 0.0, doc-size: 0.0, op-cost: 0.0, total-cost: 0.0]
                    -- EMPTY_TUPLE_SOURCE  |PARTITIONED|
```

</details>

## Summary Table

| Query | Variant | Time (s) | Key Pushdown Behavior |
|---|---:|---:|---|
| Q1 `MIN(st_x(g)), MIN(Date)` | Baseline | 19.14 | Projection pushdown to `{g, Date}` |
| Q1 `MIN(st_x(g)), MIN(Date)` | Skip projection | 21.04 | Full tuple scan |
| Q2 `MIN(st_x(g))` | Baseline | 5.522 | Projection pushdown to `{g}` |
| Q2 `MIN(st_x(g))` | Skip projection | 18.935 | Full tuple scan |
| Q3 `MIN(Date)` | Baseline | 14.05 | Projection pushdown to `{Date}` |
| Q3 `MIN(Date)` | Skip projection | 18.15 | Full tuple scan |
| Q4 `COUNT(*)` | Baseline | 3.82 | Projection pushdown to `{}` |
| Q4 `COUNT(*)` | Skip projection | 18.3 | Full tuple scan |
| Q5 spatial count | Baseline | 4.08 | Projection pushdown to `{g}` + MBR filter pushdown |
| Q5 spatial count | Skip filter | 6.101 | Projection pushdown only |
| Q5 spatial count | Skip projection | 12.93 | Filter pushdown only |
| Q5 spatial count | Skip both | 20.29 | No pushdowns |
| Q5 spatial count on ADM | ADM baseline | 35.96 | No shapefile-native pushdown |

## One-Line Interpretation

- **Projection pushdown** reduces scan cost by reading only the attributes needed by the query.
- **Filter pushdown** reduces scan cost by pruning non-qualifying records as early as possible, here via the pushed spatial MBR filter.