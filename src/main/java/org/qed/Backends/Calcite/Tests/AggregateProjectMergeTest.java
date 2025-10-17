package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class AggregateProjectMergeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var empTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("VARCHAR", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("DATE", true), false),
            Tuple.of(RelType.fromString("DECIMAL", true), false),
            Tuple.of(RelType.fromString("DECIMAL", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(empTable);
        
        var empScan = builder.scan(empTable.getName()).build();

        var before = builder
            .push(empScan)
            .project(
                builder.field(7),
                builder.field(0),
                builder.field(5)
            )
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(1)),
                builder.sum(builder.field(2))
            )
            .project(
                builder.field(0),
                builder.field(2),
                builder.field(1)
            )
            .build();

        var after = builder
            .push(empScan)
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(7)),
                builder.sum(builder.field(5))
            )
            .project(
                builder.field(1),
                builder.field(0),
                builder.field(2)
            )
            .project(
                builder.field(0),
                builder.field(2),
                builder.field(1)
            )
            .build();
            
        var runner = CalciteTester.loadRules(org.qed.Backends.Calcite.Generated.AggregateProjectMerge.Config.DEFAULT.toRule(), org.qed.Backends.Calcite.Generated.ProjectMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running AggregateProjectMerge comprehensive test...");
        runTest();
    }
}