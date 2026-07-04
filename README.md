# Modak connectors

Client surfaces for [Modak](https://github.com/Modak-Labs/modak), speaking the
public [seam protocol](https://modak-labs.github.io/modak/reference/seam/).

- `modak-connector`: engine-agnostic seam client over JDBC, the building block
  for every external consumer.
- `modak-spark`: Spark connector, SQL and DataFrames over both tiers.

Trino and Flink connectors will land here as well.

This repository is included in the main repo as a submodule at
`worker/connectors/` and builds as part of that Maven reactor:

```bash
git clone --recurse-submodules https://github.com/Modak-Labs/modak && cd modak
mvn -f worker/pom.xml package
```

There are no published artifacts yet; the submodule pin in the main repo is
the compatibility contract. Once the seam protocol stabilizes, releases move
to Maven and the submodule goes away.
