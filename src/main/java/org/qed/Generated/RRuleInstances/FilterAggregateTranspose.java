package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.qed.RelRN;
import org.qed.RelType;
import org.qed.RexRN;
import org.qed.RRule;

public record FilterAggregateTranspose() implements RRule {

    // Define a source relation with at least two columns.
    static final RelRN source = RelRN.scan("Source", "Source_Type");

    // Define the grouping key for the aggregation, using the first column of the source.
    static final Seq<RexRN> groupSet = Seq.of(source.field(0));

    // Define an aggregate function call, e.g., SUM on the second column.
    static final Seq<RelRN.AggCall> aggCalls = Seq.of(
            new RelRN.AggCall("SUM", false, RelType.fromString("INTEGER", true), Seq.of(source.field(1)))
    );

    // Define the aggregate node. Its output schema will be (group_key_type, sum_type).
    static final RelRN aggregate = source.aggregate(groupSet, aggCalls);

    // Define a predicate that filters on the grouping key (the first column of the aggregate's output).
    static final RexRN pred = aggregate.field(0).pred("pred");

    /**
     * The 'before' pattern represents a Filter applied on top of an Aggregate.
     */
    @Override
    public RelRN before() {
        return aggregate.filter(pred);
    }

    /**
     * The 'after' pattern represents the transposed operators, where the Aggregate
     * is applied on top of a Filter.
     */
    @Override
    public RelRN after() {
        // The predicate is rewritten to apply to the aggregate's input (the source).
        // The filter condition was on the first field of the aggregate's output (the group key),
        // which corresponds to the first field of the original source.
        RelRN filteredSource = source.filter(source.field(0).pred("pred"));

        // The aggregation is now applied to the filtered source.
        return filteredSource.aggregate(groupSet, aggCalls);
    }
}