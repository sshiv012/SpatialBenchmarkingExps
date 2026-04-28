# Spatial Benchmarking Experiments

This repository collects spatial benchmark results, query text, optimization notes, and supporting scripts for the AsterixDB and Sedona experiments. Use [SQLPP_Query_Execution_Times.md](SQLPP_Query_Execution_Times.md) as the primary reference for the latest query timings and configuration notes.

## Files

| File | Description |
|---|---|
| [Benchmark_Execution_Times.xlsx](Benchmark_Execution_Times.xlsx) | Original Excel workbook with the benchmark timing tables. This is useful as the source spreadsheet, but it does not include the later optimized PBSM spatial join settings/timings or the best corrected Sedona timing. |
| [SQLPP_Query_Execution_Times.md](SQLPP_Query_Execution_Times.md) | Primary Markdown reference. It extends the workbook with the missing SQL++ query text, optimized PBSM join configuration, profiling breakdown, best recorded AsterixDB timing, and Sedona comparison notes. |
| [ShapefileTests.md](ShapefileTests.md) | Notes for the Chicago shapefile external dataset pushdown evaluation, including DDL, benchmark queries, execution times, and logical plans for shapefile-backed versus ADM/JSON-backed external datasets. |
| [SedonaJoinTestAPR2026.scala](SedonaJoinTestAPR2026.scala) | Spark/Sedona script for Ahmed to review. It registers Sedona SQL, prepares buildings and all-nodes views from HDFS, runs the containment join, and logs Spark runtime, plans, memory snapshots, and action timings. |
| [LICENSE](LICENSE) | Repository license. |
| [.gitattributes](.gitattributes) | Git metadata/configuration for repository file handling. |

## Primary Timing Reference

[Benchmark_Execution_Times.xlsx](Benchmark_Execution_Times.xlsx) is not the final source of truth for the optimized spatial join results. It does not contain the later optimized PBSM join execution times, the associated AsterixDB compiler settings, or the corrected best Sedona timing.

The workbook also still has the note `Need to check this once` for the Sedona result. During reruns, the earlier Sedona script behavior was identified as a source of confusion: the previous script ran `show()` first and then `collect()`, which executes the same aggregate query twice when the DataFrame is not cached. A later rerun using just `collect()` completed in under `1800 s` / about `30 min` with a valid result. The earlier slower timing is believed to have been affected by resource contention and GC pressure.

For reporting, use [SQLPP_Query_Execution_Times.md](SQLPP_Query_Execution_Times.md) because it includes the missing optimized PBSM details and the notes needed to interpret the Sedona comparison correctly.

## Related Notes

- Main query/timing reference: [SQLPP_Query_Execution_Times.md](SQLPP_Query_Execution_Times.md)
- Shapefile pushdown experiments: [ShapefileTests.md](ShapefileTests.md)

