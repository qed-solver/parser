package org.cosette;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.Map;

public class RelVariableTable extends AbstractTable {

    private final String name;

    private final ImmutableMap<String, RelType> columns;

    public RelVariableTable(String identifier, ImmutableMap<String, RelType> columnTypes) {
        name = identifier;
        columns = columnTypes;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(Seq.from(columns.iterator()).stream().map(entry -> Map.entry(entry._1, (RelDataType) entry._2)).toList());
    }

    public String getName() {
        return name;
    }
}