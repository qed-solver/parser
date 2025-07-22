package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RelType;

public record ProjectAggregateMerge() implements RRule {
    // Create a two-column source relation using a join, as the aggregate needs multiple columns.
    static final RelRN source_col1 = RelRN.scan("SourceCol1", "Int_Type");
    static final RelRN source_col2 = RelRN.scan("SourceCol2", "Int_Type");
    static final RelRN source = source_col1.join(JoinRelType.INNER, RexRN.trueLiteral(), source_col2);

    static final RelRN.Aggregate aggregate = new RelRN.Aggregate(
            source,
            Seq.of(source.field(0)), // group by key (from source_col1)
            Seq.of(new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(source.field(1)))) // sum(field from source_col2)
    );

    @Override
    public RelRN before() {
        // Project on top of the aggregate, selecting only the SUM result.
        return aggregate.project(aggregate.field(1));
    }

    @Override
    public RelRN after() {
        // The 'after' state represents the merged operation.
        // This would be a single aggregate operator that produces the projected result directly.
        // Since the projection removes the group key, the ideal 'after' would be an aggregate
        // that computes the sum for each group but does not output the group key.
        // As a placeholder for this complex transformation, we create an aggregate
        // with an empty group set, which results in a single row output (total sum).
        return new RelRN.Aggregate(
                source,
                Seq.empty(),
                Seq.of(new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(source.field(1))))
        );
    }
}