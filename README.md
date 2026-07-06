# TierDB connectors

Client surfaces for [TierDB](https://github.com/Modak-Labs/tierdb), speaking the
public [seam protocol](https://tierdb-labs.github.io/tierdb/reference/seam/).

- `tierdb-connector`: engine-agnostic seam client over JDBC, the building block
  for every external consumer.
- `tierdb-spark`: Spark connector, SQL and DataFrames over both tiers.
- Trino and Flink connectors coming soon.
