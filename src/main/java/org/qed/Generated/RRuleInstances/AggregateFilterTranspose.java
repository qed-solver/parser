package org.qed.Generated.RRuleInstances;

import kala.collection.Seq;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;
import org.qed.RelType;

public record AggregateFilterTranspose() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN.GroupBy groupExpr = source.groupBy("groupByName");

    @Override
    public RelRN before() {
        return source.filter(groupExpr.pred("pred"))
                     .aggregate(groupExpr, source.aggCall("aggName"));
    }
    
    @Override
    public RelRN after() {
        RelRN aggregated = source.aggregate(groupExpr, source.aggCall("aggName"));
        return aggregated.filter(aggregated.field(0).pred("pred"));
    }
}