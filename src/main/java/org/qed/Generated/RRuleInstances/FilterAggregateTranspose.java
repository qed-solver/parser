package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.qed.RelRN;
import org.qed.RelType;
import org.qed.RexRN;
import org.qed.RRule;

public record FilterAggregateTranspose() implements RRule {

    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final Seq<RexRN> groupSet = Seq.of(source.field(0));
    static final Seq<RelRN.AggCall> aggCalls = Seq.of(
            new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(source.field(1)))
    );
    static final RelRN aggregate = source.aggregate(groupSet, aggCalls);
    static final RexRN pred = aggregate.field(0).pred("pred");

    @Override
    public RelRN before() {
        return aggregate.filter(pred);
    }

    @Override
    public RelRN after() {
        RelRN filteredSource = source.filter(source.field(0).pred("pred"));
        return filteredSource.aggregate(groupSet, aggCalls);
    }
}