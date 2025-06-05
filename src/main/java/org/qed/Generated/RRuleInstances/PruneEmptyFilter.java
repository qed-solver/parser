package org.qed.Generated.RRuleInstances;

import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record PruneEmptyFilter() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN cond = source.pred("filter_cond");

    @Override
    public RelRN before() {
        return source.empty().filter(cond);
    }

    @Override
    public RelRN after() {
        return source.empty();
    }
}
