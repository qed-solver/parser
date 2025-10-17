package org.qed.RRuleInstances;

import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;

public record FilterReduceFalse() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");

    @Override
    public RelRN before() {
        return source.filter(RexRN.falseLiteral());
    }

    @Override
    public RelRN after() {
        return source.empty();
    }
}
