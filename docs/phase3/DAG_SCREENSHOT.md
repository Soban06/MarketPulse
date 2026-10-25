# One screenshot still needed for the report

Browser automation could not safely verify the browser address on this computer,
so it was stopped. The evidence notebook itself ran successfully through the API.
Its HTML export and physical plan are saved under docs/run-evidence.

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
The report already distinguishes the captured EXPLAIN plan from the missing
runtime graph screenshot. If the instructor requires literal Spark UI, obtain a
classic-compute environment rather than relabelling Query Profile as Spark UI.
