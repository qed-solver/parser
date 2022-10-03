package org.cosette;

import kala.collection.Seq;
import kala.collection.Set;
import kala.collection.immutable.ImmutableMap;
import kala.collection.immutable.ImmutableSeq;
import kala.collection.immutable.ImmutableSet;
import kala.collection.mutable.MutableHashMap;
import kala.collection.mutable.MutableMap;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.util.ImmutableBitSet;

public class RelPruner implements RelFolder {
    private final MutableMap<RelOptTable, Tuple2<CosetteTable, RelDataType>> cache = MutableMap.create();
    ImmutableMap<String, ImmutableSet<Integer>> usages;

    public RelPruner(ImmutableMap<String, ImmutableSet<Integer>> usages) {
        this.usages = usages;
    }

    public RelNode post(RelNode rel) {
        return rel;
    }

    private LogicalTableScan prune(LogicalTableScan scan) {
        var table = scan.getTable();
        CosetteTable cosTable;
        RelDataType rowType;
        if (!cache.containsKey(table)) {
            var fieldList = table.getRowType().getFieldList();
            var fields = usages.get(RelScanner.getName(scan)).toSeq().sorted();
            var columns = ImmutableMap.from(fields.map(table.getRowType().getFieldNames()::get).zip(fields.map(i -> fieldList.get(i).getType())));
            var keys = table.getKeys() == null
                    ? ImmutableSet.<ImmutableBitSet>empty()
                    : ImmutableSet.from(Seq.from(table.getKeys()).filter(ks -> ImmutableSet.from(ks).removedAll(fields).isEmpty())
                    .map(ks -> ImmutableBitSet.of(ImmutableSet.from(ks).map(fields::indexOf))));
            var qName = table.getQualifiedName();
            cosTable = new CosetteTable(qName.get(qName.size() - 1), columns, keys, Set.empty());
            rowType = new RelRecordType(fields.map(fieldList::get).asJava());
            cache.put(table, Tuple.of(cosTable, rowType));
        } else {
            var p = cache.get(table);
            cosTable = p._1;
            rowType = p._2;
        }
        var t = RelOptTableImpl.create(table.getRelOptSchema(), rowType, table.getQualifiedName(), cosTable, table::getExpression);
        return LogicalTableScan.create(scan.getCluster(), t, scan.getHints());
    }

    @Override
    public RelNode apply(RelNode rel) {
        return switch (rel) {
            case LogicalProject project
                    && (project.getInput() instanceof LogicalTableScan r)
                    && Seq.from(project.getProjects()).allMatch(col -> col instanceof RexInputRef) -> {
                var fin = usages.get(RelScanner.getName(r)).toSeq().sorted();
                var ids = Seq.from(project.getProjects()).map(ref -> ((RexInputRef) ref).getIndex());
                var scan = prune(r);
                if (fin.sameElements(ids)) {
                    yield scan;
                }
                ImmutableSeq<RexNode> fields = ids.zip(project.getProjects()).map(p -> new RexInputRef(fin.indexOf(p._1), p._2.getType()));
                yield project.copy(project.getTraitSet(), scan, fields.asJava(), project.getRowType());
            }
            case LogicalTableScan r -> {
                if (!usages.containsKey(RelScanner.getName(r))
                        || !usages.get(RelScanner.getName(r)).toSeq().sorted().sameElements(ImmutableSeq.fill(r.getTable().getRowType().getFieldCount(), i -> i))) {
                    throw new IllegalStateException("Illegal raw occurrence of TableScan");
                }
                yield prune(r);
            }
            default -> RelFolder.super.apply(rel);
        };
    }
}

record RelScanner(MutableMap<String, ImmutableSet<Integer>> usages) {
    public RelScanner() {
        this(new MutableHashMap<>());
    }

    public static String getName(LogicalTableScan tableScan) {
        var qName = tableScan.getTable().getQualifiedName();
        return qName.get(qName.size() - 1);
    }

    public void scan(RelNode rel) {
        rel.accept(new RexScanner());
        switch (rel) {
            case LogicalProject project
                    && (project.getInput() instanceof LogicalTableScan r)
                    && Seq.from(project.getProjects()).allMatch(col -> col instanceof RexInputRef) -> {
                var ids = Seq.from(project.getProjects()).map(col -> ((RexInputRef) col).getIndex());
                var name = getName(r);
                usages.put(name, usages.getOrDefault(name, ImmutableSet.empty()).addedAll(ids));
            }
            case LogicalTableScan r -> {
                var ids = ImmutableSet.from(ImmutableSeq.fill(r.getTable().getRowType().getFieldCount(), i -> i));
                usages.put(getName(r), ids);
            }
            default -> rel.getInputs().forEach(this::scan);
        }
    }

    private class RexScanner extends RexShuttle {
        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            scan(subQuery.rel);
            return super.visitSubQuery(subQuery);
        }
    }
}
