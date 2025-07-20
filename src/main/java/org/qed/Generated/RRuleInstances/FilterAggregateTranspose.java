package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RelType;

public record FilterAggregateTranspose() implements RRule {
    static final RelRN source_col1 = RelRN.scan("Source1", "Int_Type");
    static final RelRN source_col2 = RelRN.scan("Source2", "Int_Type");
    static final RelRN source = source_col1.join(JoinRelType.INNER, RexRN.trueLiteral(), source_col2);
    static final RelRN.Aggregate aggregate = new RelRN.Aggregate(source, Seq.of(source.field(0)), Seq.of(new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(source.field(1)))));
    static final RexRN pushedCondition = aggregate.field(0).pred("pushed_cond");
    static final RexRN remainingCondition = aggregate.field(1).pred("remaining_cond");

    @Override
    public RelRN before() {
        return aggregate.filter(RexRN.and(pushedCondition, remainingCondition));
    }

    @Override
    public RelRN after() {
        RelRN filteredSource = source.filter(source.field(0).pred("pushed_cond"));
        RelRN.Aggregate newAggregate = new RelRN.Aggregate(filteredSource,  Seq.of(filteredSource.field(0)),  Seq.of(new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(filteredSource.field(1)))));
        return newAggregate.filter(newAggregate.field(1).pred("remaining_cond"));
    }
}