package org.cosette;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBiVisitor;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.atomic.AtomicInteger;

public class RexVariable extends RexNode {

    private static final AtomicInteger ID = new AtomicInteger(0);

    private final int id;

    public RexVariable() {
        id = ID.getAndIncrement();
    }

    public int getId() {
        return id;
    }

    @Override
    public RelDataType getType() {
        return new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.ANY);
    }

    @Override
    public <R> R accept(RexVisitor<R> visitor) {
        if (visitor instanceof RexJSONVisitor rexJSONVisitor) {
            return (R) rexJSONVisitor.visit(this);
        }
        return null;
    }

    @Override
    public <R, P> R accept(RexBiVisitor<R, P> visitor, P arg) {
        return null;
    }

    @Override
    public boolean equals(@Nullable Object obj) {
        return false;
    }

    @Override
    public int hashCode() {
        return id;
    }

    @Override
    public String toString() {
        return "RexVar" + id;
    }
}
