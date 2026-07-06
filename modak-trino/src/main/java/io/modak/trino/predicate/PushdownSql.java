package io.modak.trino.predicate;

import io.modak.connector.predicate.SqlPredicates;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import java.util.Optional;

public final class PushdownSql {

    private PushdownSql() {}

    public static String quoteIdent(String name) {
        return SqlPredicates.quoteIdent(name);
    }

    public static Optional<String> where(TupleDomain<ColumnHandle> constraint) {
        if (constraint.isNone()) {
            return Optional.of("FALSE");
        }
        return SqlPredicates.where(ConstraintAdapter.from(constraint));
    }
}
