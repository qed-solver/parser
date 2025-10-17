package org.qed.RRuleInstances;

import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RexRN;

public record AggregateProjectMerge() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN proj = source.proj("proj", "Project_Type");

    @Override
    public RelRN before() {
        return source.project(proj).aggregate("groupByName", "aggName");
    }

    @Override
    public RelRN after() {
        return source.aggregate(
            proj.groupBy("groupByName"),
            proj.aggCall("aggName")
        );
    }
}