package io.modak.trino;

import io.airlift.slice.Slices;
import io.modak.common.RowBatchData.Column;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class TrinoTypes {

    private TrinoTypes() {}

    public static Type fromPg(Column column) {
        return switch (column.type()) {
            case BOOLEAN -> BOOLEAN;
            case LONG -> column.precision() <= 16 ? SMALLINT
                    : column.precision() <= 32 ? INTEGER : BIGINT;
            case DOUBLE -> column.precision() > 0 && column.precision() <= 24 ? REAL : DOUBLE;
            case DECIMAL -> DecimalType.createDecimalType(column.precision(), column.scale());
            case TEXT -> VARCHAR;
            case UUID -> VARCHAR;
            case DATE -> DATE;
            case TIMESTAMP -> TIMESTAMP_TZ_MICROS;
            case BINARY -> VARBINARY;
        };
    }

    public static void write(Type type, BlockBuilder builder, Object value) {
        if (value == null) {
            builder.appendNull();
            return;
        }
        if (type == BOOLEAN) {
            BOOLEAN.writeBoolean(builder, (Boolean) value);
        } else if (type == SMALLINT) {
            SMALLINT.writeLong(builder, ((Number) value).longValue());
        } else if (type == INTEGER) {
            INTEGER.writeLong(builder, ((Number) value).longValue());
        } else if (type == BIGINT) {
            BIGINT.writeLong(builder, ((Number) value).longValue());
        } else if (type == REAL) {
            REAL.writeLong(builder, Float.floatToRawIntBits(((Number) value).floatValue()));
        } else if (type == DOUBLE) {
            DOUBLE.writeDouble(builder, ((Number) value).doubleValue());
        } else if (type instanceof DecimalType decimal) {
            BigDecimal scaled = ((BigDecimal) value)
                    .setScale(decimal.getScale(), RoundingMode.HALF_UP);
            if (decimal.isShort()) {
                decimal.writeLong(builder, scaled.unscaledValue().longValueExact());
            } else {
                decimal.writeObject(builder, Int128.valueOf(scaled.unscaledValue()));
            }
        } else if (type == VARCHAR) {
            VARCHAR.writeSlice(builder, Slices.utf8Slice(value.toString()));
        } else if (type == VARBINARY) {
            VARBINARY.writeSlice(builder, Slices.wrappedBuffer((byte[]) value));
        } else if (type == DATE) {
            DATE.writeLong(builder, ((LocalDate) value).toEpochDay());
        } else if (type == TIMESTAMP_TZ_MICROS) {
            Instant instant = ((OffsetDateTime) value).toInstant();
            TIMESTAMP_TZ_MICROS.writeObject(builder,
                    LongTimestampWithTimeZone.fromEpochMillisAndFraction(
                            instant.toEpochMilli(),
                            (instant.getNano() % 1_000_000) * 1000,
                            TimeZoneKey.UTC_KEY));
        } else {
            throw new TrinoException(NOT_SUPPORTED, "cannot write type " + type);
        }
    }
}
