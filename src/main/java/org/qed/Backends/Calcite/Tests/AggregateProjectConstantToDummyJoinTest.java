package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class AggregateProjectConstantToDummyJoinTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create source table with 2 fields
        var sourceTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // field 0
            Tuple.of(RelType.fromString("INTEGER", true), false)   // field 1
        ));
        builder.addTable(sourceTable);
        
        var scan = builder.scan(sourceTable.getName()).build();
        
        // Before: source -> project adds literals -> aggregate groups by literals
        // Project: [field(0), literal(true), literal(2024), field(1)]
        // Aggregate: group by [field(1)=true, field(2)=2024, field(0)], avg(field(3))
        var before = builder
            .push(scan)
            .project(
                builder.field(0),
                builder.literal(true),
                builder.literal(2024),
                builder.field(1)
            )
            .aggregate(
                builder.groupKey(builder.field(1), builder.field(2), builder.field(0)),
                builder.avg(builder.field(3))
            )
            .build();
        
        // After: source -> join with dummy values table -> project replaces literals with dummy fields -> aggregate
        // Join adds dummy table with [true, 2024]
        // After join: [source.f0, source.f1, dummy.col0=true, dummy.col1=2024]
        // Project: [field(0)=source.f0, field(2)=dummy.true, field(3)=dummy.2024, field(1)=source.f1]
        // Aggregate: group by [field(1)=dummy.true, field(2)=dummy.2024, field(0)=source.f0], avg(field(3))
        var after = builder
            .push(scan)
            .values(new String[]{"col0", "col1"}, true, 2024)
            .join(JoinRelType.INNER, builder.literal(true))
            .project(
                builder.field(0),  // source.field(0)
                builder.field(2),  // dummy.col0 (true)
                builder.field(3),  // dummy.col1 (2024)
                builder.field(1)   // source.field(1)
            )
            .aggregate(
                builder.groupKey(builder.field(1), builder.field(2), builder.field(0)),
                builder.avg(builder.field(3))
            )
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.AggregateProjectConstantToDummyJoin.Config.DEFAULT.toRule(), 1
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running AggregateProjectConstantToDummyJoin test...");
        runTest();
    }
}