package org.qed.RRuleInstances;

import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;

public record FilterReduceTrue() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");

    @Override
    public RelRN before() {
        return source.filter(RexRN.trueLiteral());
    }

    @Override
    public RelRN after() {
        return source;
    }
}
