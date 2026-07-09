package io.tierdb.connector.read;

public sealed interface Read {
    record Heap() implements Read {}

    record Seam(long t, Cold cold) implements Read {}
}
