package org.qed.RRuleInstances;

import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;

public record ProjectFilterTranspose() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN proj = source.proj("proj", "Project_Type");

    @Override
    public RelRN before() {
        return source.filter(proj.pred("pred")).project(proj);
    }

    @Override
    public RelRN after() {
        return source.project(proj).filter("pred");
    }
}
