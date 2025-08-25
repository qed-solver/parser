package org.qed.Generated.RRuleInstances;

import org.apache.calcite.rel.RelNode;
import org.qed.RelRN;
import org.qed.RRule;
import org.qed.RuleBuilder;
import org.qed.RelType;
import org.qed.RexRN;

import kala.collection.Seq;
import kala.tuple.Tuple;

public record AggregateExtractProject() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN proj = source.proj("proj", "Project_Type");

    @Override
    public RelRN before() {
        return source.aggregate(
            proj.groupBy("groupByName"),
            proj.aggCall("aggName")
        );
    }

    @Override
    public RelRN after() {
        return source.project(proj).aggregate("groupByName", "aggName");
    }
}