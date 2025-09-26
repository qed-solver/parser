package org.qed.RRuleInstances;

import org.qed.RRule;
import org.qed.RelRN;
import org.qed.RexRN;

public record PruneEmptyProject() implements RRule {
    static final RelRN source = RelRN.scan("Source", "Source_Type");
    static final RexRN proj = source.proj("proj", "Project_Type");

    @Override
    public RelRN before() {
        return source.empty().project(proj);
    }

    @Override
    public RelRN after() {
        return source.empty();
    }
}
