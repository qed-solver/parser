package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class UnionPullUpConstantsTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create source tables with 3 fields each
        var leftTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(leftTable);
        
        var rightTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(rightTable);
        
        var scanLeft = builder.scan(leftTable.getName()).build();
        var scanRight = builder.scan(rightTable.getName()).build();
        
        // Before: both sides project with constant in middle
        // Left: [field(0), literal("ACTIVE"), field(2)]
        // Right: [field(0), literal("ACTIVE"), field(2)]
        // Union: all 3 columns including the constant
        var before = builder
            .push(scanLeft)
            .project(
                builder.field(0),
                builder.alias(builder.literal("ACTIVE"), "status"),
                builder.field(2)
            )
            .push(scanRight)
            .project(
                builder.field(0),
                builder.alias(builder.literal("ACTIVE"), "status"),
                builder.field(2)
            )
            .union(true, 2)
            .build();
        
        // After: pull constant to top
        // Left reduced: [field(0), field(2)]
        // Right reduced: [field(0), field(2)]
        // Union: only 2 columns
        // Top project adds constant back: [field(0), literal("ACTIVE"), field(1)]
        var after = builder
            .push(scanLeft)
            .project(
                builder.field(0),
                builder.field(2)
            )
            .push(scanRight)
            .project(
                builder.field(0),
                builder.field(2)
            )
            .union(true, 2)
            .project(
                builder.field(0),
                builder.alias(builder.literal("ACTIVE"), "status"),
                builder.field(1)
            )
            .build();
            
        var runner = CalciteTester.loadRules(
            org.qed.Backends.Calcite.Generated.UnionPullUpConstants.Config.DEFAULT.toRule(), org.qed.Backends.Calcite.Generated.ProjectMerge.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running UnionPullUpConstants test...");
        runTest();
    }
}