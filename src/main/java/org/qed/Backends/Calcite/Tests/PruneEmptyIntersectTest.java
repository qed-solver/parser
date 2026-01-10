package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class PruneEmptyIntersectTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create two tables with compatible schemas
        var tableA = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(tableA);
        
        var tableB = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(tableB);
        
        var scanA = builder.scan(tableA.getName()).build();
        var scanB = builder.scan(tableB.getName()).build();
        
        // Before: A INTERSECT DISTINCT (Empty B)
        var before = builder
            .push(scanA)
            .push(scanB)
            .empty()
            .intersect(false, 2)
            .build();
        
        // After: (Empty A) INTERSECT DISTINCT (Empty B)
        var after = builder
            .push(scanA)
            .empty()
            .push(scanB)
            .empty()
            .intersect(false, 2)
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.PruneEmptyIntersect.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running PruneEmptyIntersect test...");
        runTest();
    }
}