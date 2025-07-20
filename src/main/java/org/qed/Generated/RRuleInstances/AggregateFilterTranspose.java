package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RelType;

public record AggregateFilterTranspose() implements RRule {
    static final RelRN source_col1 = RelRN.scan("Source1", "Int_Type");
    static final RelRN source_col2 = RelRN.scan("Source2", "Int_Type");
    static final RelRN source = source_col1.join(JoinRelType.INNER, RexRN.trueLiteral(), source_col2);
    static final RexRN filterCondition = source.field(0).pred("cond");

    @Override
    public RelRN before() {
        RelRN filteredSource = source.filter(filterCondition);
        return new RelRN.Aggregate(
                filteredSource,
                Seq.of(filteredSource.field(0)),
                Seq.of(new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(filteredSource.field(1))))
        );
    }
    @Override
    public RelRN after() {
        RelRN.Aggregate aggregateOnSource = new RelRN.Aggregate(source, Seq.of(source.field(0)), Seq.of(new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(source.field(1)))));
        return aggregateOnSource.filter(aggregateOnSource.field(0).pred("cond"));
    }
}