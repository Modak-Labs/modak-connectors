package io.modak.trino.predicate;

import io.modak.connector.predicate.IcebergPredicates;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.TupleDomain;
import java.util.Optional;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

public final class IcebergFilters {

    private IcebergFilters() {}

    public static Optional<Expression> expression(TupleDomain<ColumnHandle> constraint) {
        if (constraint.isNone()) {
            return Optional.of(Expressions.alwaysFalse());
        }
        return IcebergPredicates.expression(ConstraintAdapter.from(constraint));
    }
}
