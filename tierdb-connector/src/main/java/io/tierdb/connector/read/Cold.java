package io.tierdb.connector.read;

import io.tierdb.lake.access.LakeScan;

public sealed interface Cold {
    record Delta() implements Cold {}

    record Live(LakeScan scan) implements Cold {}

    record Merge(LakeScan scan) implements Cold {}
}
