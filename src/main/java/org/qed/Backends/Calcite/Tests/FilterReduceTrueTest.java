package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class FilterReduceTrueTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create a simple table
        var sourceTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(sourceTable);
        
        var scan = builder.scan(sourceTable.getName()).build();
        
        // Before: scan + filter(true)
        var before = builder
            .push(scan)
            .filter(builder.literal(true))
            .build();
        
        // After: just scan (filter is removed)
        var after = builder
            .push(scan)
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.FilterReduceTrue.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running FilterReduceTrue test...");
        runTest();
    }
}