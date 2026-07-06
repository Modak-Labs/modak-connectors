package io.modak.trino.predicate;

import io.airlift.slice.Slice;
import io.modak.common.RowBatchData.Column;
import io.modak.common.RowBatchData.ColumnType;
import io.modak.connector.predicate.ColumnConstraint;
import io.modak.connector.predicate.ColumnConstraint.OnlyNull;
import io.modak.connector.predicate.ColumnConstraint.Ranges;
import io.modak.connector.predicate.ColumnConstraint.ValueRange;
import io.modak.trino.ModakColumnHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class ConstraintAdapter {

    private static final Set<ColumnType> PUSHABLE = EnumSet.of(
            ColumnType.BOOLEAN, ColumnType.LONG, ColumnType.DOUBLE,
            ColumnType.DATE, ColumnType.TEXT);

    private ConstraintAdapter() {}

    static Map<Column, ColumnConstraint> from(TupleDomain<ColumnHandle> constraint) {
        Map<ColumnHandle, Domain> domains = constraint.getDomains().orElse(Map.of());
        Map<Column, ColumnConstraint> constraints = new LinkedHashMap<>();
        for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
            ModakColumnHandle handle = (ModakColumnHandle) entry.getKey();
            ColumnType kind = ColumnType.valueOf(handle.kind());
            if (!PUSHABLE.contains(kind)) {
                continue;
            }
            decode(kind, entry.getValue())
                    .ifPresent(c -> constraints.put(new Column(handle.name(), kind), c));
        }
        return constraints;
    }

    private static Optional<ColumnConstraint> decode(ColumnType kind, Domain domain) {
        if (domain.isOnlyNull()) {
            return Optional.of(new OnlyNull());
        }
        List<ValueRange> ranges = new ArrayList<>();
        try {
            for (Range range : domain.getValues().getRanges().getOrderedRanges()) {
                ranges.add(decodeRange(kind, range));
            }
        } catch (RuntimeException e) {
            return Optional.empty();
        }
        return Optional.of(new Ranges(ranges, domain.isNullAllowed()));
    }

    private static ValueRange decodeRange(ColumnType kind, Range range) {
        Object low = range.isLowUnbounded() ? null : literal(kind, range.getLowBoundedValue());
        Object high = range.isHighUnbounded() ? null : literal(kind, range.getHighBoundedValue());
        boolean lowInclusive = !range.isLowUnbounded() && range.isLowInclusive();
        boolean highInclusive = !range.isHighUnbounded() && range.isHighInclusive();
        return new ValueRange(low, lowInclusive, high, highInclusive);
    }

    private static Object literal(ColumnType kind, Object value) {
        return switch (kind) {
            case BOOLEAN -> (Boolean) value;
            case LONG -> (Long) value;
            case DOUBLE -> (Double) value;
            case DATE -> LocalDate.ofEpochDay((Long) value);
            case TEXT -> ((Slice) value).toStringUtf8();
            default -> throw new IllegalArgumentException("not pushable: " + kind);
        };
    }
}
