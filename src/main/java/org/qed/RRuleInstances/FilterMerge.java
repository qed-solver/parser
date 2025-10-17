package org.qed.RRuleInstances;

import org.qed.RelRN;
import org.qed.RexRN;
import org.qed.RRule;

public record FilterMerge() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN inner = source.pred("inner");
    static final RexRN outer = source.pred("outer");

    @Override
    public RelRN before() {
        return source.filter(inner).filter(outer);
    }

    @Override
    public RelRN after() {
        return source.filter(RexRN.and(inner, outer));
    }
}
