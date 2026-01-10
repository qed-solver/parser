package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class ProjectMergeTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create a source table with 3 fields
        var sourceTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),  // field 0
            Tuple.of(RelType.fromString("INTEGER", true), false),  // field 1
            Tuple.of(RelType.fromString("INTEGER", true), false)   // field 2
        ));
        builder.addTable(sourceTable);
        
        var scan = builder.scan(sourceTable.getName()).build();
        
        // Before: scan → project(f0, f1) → project(field(1), field(0))
        // Inner projection selects f0, f1 from source
        // Outer projection reorders them to f1, f0
        var before = builder
            .push(scan)
            .project(
                builder.field(0),  // f0
                builder.field(1)   // f1
            )
            .project(
                builder.field(1),  // selects field 1 from inner projection (which is f1)
                builder.field(0)   // selects field 0 from inner projection (which is f0)
            )
            .build();
        
        // After: scan → project(f1, f0)
        // Single merged projection directly selects f1, f0 from source
        var after = builder
            .push(scan)
            .project(
                builder.field(1),  // f1 directly from source
                builder.field(0)   // f0 directly from source
            )
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.ProjectMerge.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running ProjectMerge test...");
        runTest();
    }
}