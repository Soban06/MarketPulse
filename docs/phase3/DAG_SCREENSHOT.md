# Captured DAG screenshot and reproduction steps

Soban supplied the actual Query Profile graph and matching performance summary.
Both original screenshots are saved in docs/report/images and the graph is embedded
in the report with an operator-by-operator explanation. The runtime graph shows
Scan Table (328 ms), two Grouping Aggregate stages (123 ms and 2 ms), Shuffle
(38 ms), Columnar To Row (1 ms), Limit and Result Query Stage (0 ms each).
The summary shows 1,230,497 rows and 21.20 MB read, with 1.312 seconds query duration.
The Limit is a 1,000-row notebook preview, not the full monthly fact cardinality.

**Still needed:** instructor acceptance of Query Profile in place of Spark UI.
The steps below reproduce the already captured evidence if another image is needed.

1. Open the [completed evidence job](https://dbc-43683058-46b3.cloud.databricks.com/jobs/95255830185193/runs/436619686310315?o=7474657227153277).
2. Open the **evidence** task and its notebook output.
3. Find the cell beneath **Gold aggregation DAG**. Open **See performance** /
   **Query Profile**, select the monthly aggregation query, then show the graph.
4. Capture a readable screenshot. Save it as `docs/report/images/gold-query-profile.png`.
5. Set `dag.image` in docs/report/evidence.json to that repository-relative path.
6. Confirm with the instructor that Query Profile is accepted for Free Edition,
   then set `dag.instructor_accepted_query_profile=true` only when actually accepted.

The captured initial physical plan includes:

- **PhotonScan parquet:** reads five required columns from Gold daily facts. This
  demonstrates planned column pruning, not a predicate filter (this query has no WHERE).
- **PhotonProject:** derives the month grouping key.
- **PhotonGroupingAgg:** partial count/sum/average/variance/correlation statistics.
- **PhotonShuffleExchangeSink / Source:** redistribute partial groups by ticker/month.
- **PhotonGroupingAgg:** combine partial statistics into final ticker-month results.
- **AdaptiveSparkPlan:** runtime may adapt the initial plan; inspect the actual graph.

The executed query produced the monthly result. Explain actual profile operators
and scan/shuffle statistics visible in the screenshot; do not invent bytes or time.
The report distinguishes the initial EXPLAIN plan from the supplied runtime
graph screenshot. If the instructor requires literal Spark UI, obtain a
classic-compute environment rather than relabelling Query Profile as Spark UI.
