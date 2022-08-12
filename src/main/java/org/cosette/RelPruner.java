package org.cosette;

import kala.collection.Seq;
import kala.collection.immutable.ImmutableSet;
import kala.collection.mutable.MutableHashMap;
import kala.collection.mutable.MutableMap;
import kala.control.Option;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;

public record RelPruner(MutableMap<String, Option<ImmutableSet<Integer>>> usages) {
    public RelPruner() {
        this(new MutableHashMap<>());
    }

    private static String getName(LogicalTableScan tableScan) {
        var qName = tableScan.getTable().getQualifiedName();
        return qName.get(qName.size() - 1);
    }

    public void scan(RelNode rel) {
        rel.accept(new RexPruner());
        switch (rel) {
            case LogicalProject project
                    && (project.getInput() instanceof LogicalTableScan r)
                    && Seq.from(project.getProjects()).allMatch(col -> col instanceof RexInputRef) -> {
                var ids = Seq.from(project.getProjects()).map(col -> ((RexInputRef) col).getIndex());
                var name = getName(r);
                usages.put(name, usages.getOrDefault(name, Option.some(ImmutableSet.empty()))
                        .map(old -> old.addedAll(ids)));
            }
            case LogicalTableScan r -> usages.put(getName(r), Option.none());
            default -> rel.getInputs().forEach(this::scan);
        }
    }

    private class RexPruner extends RexShuttle {
        @Override public RexNode visitSubQuery(RexSubQuery subQuery) {
            scan(subQuery.rel);
            return super.visitSubQuery(subQuery);
        }
    }
}
