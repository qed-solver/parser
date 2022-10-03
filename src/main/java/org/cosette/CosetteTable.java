package org.cosette;

import kala.collection.Map;
import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableSet;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.ImmutableBitSet;

public class CosetteTable extends AbstractTable {
    private final String name;

    private final Seq<String> columnNames;

    private final Seq<RelDataType> columnTypes;
    private final Set<ImmutableBitSet> keys;
    private final Set<RexNode> constraints;

    public CosetteTable(String identifier, Map<String, RelDataType> columns, Set<Set<String>> eligibleKeys, Set<RexNode> checkConstraints) {
        name = identifier;
        columnNames = columns.keysView().toImmutableSeq().sorted();
        columnTypes = columnNames.map(columns::get);
        keys = Set.from(eligibleKeys.map(key -> ImmutableBitSet.of(key.map(columnNames::indexOf))));
        constraints = checkConstraints;
    }

    protected CosetteTable(String identifier, Map<String, RelDataType> columns, ImmutableSet<ImmutableBitSet> eligibleKeys, Set<RexNode> checkConstraints) {
        name = identifier;
        columnNames = columns.keysView().toImmutableSeq().sorted();
        columnTypes = columnNames.map(columns::get);
        keys = eligibleKeys;
        constraints = checkConstraints;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(columnNames.zip(columnTypes).view().map(entry -> java.util.Map.entry(entry._1, entry._2)).toImmutableSeq().asJava());
    }

    @Override
    public Statistic getStatistic() {
        return Statistics.of(0, keys.toImmutableSeq().asJava());
    }

    public String getName() {
        return name;
    }

    public Seq<String> getColumnNames() {
        return columnNames;
    }

    public Seq<RelDataType> getColumnTypes() {
        return columnTypes;
    }

    public Set<ImmutableBitSet> getKeys() {
        return keys;
    }

    public Set<RexNode> getConstraints() {
        return constraints;
    }
}
