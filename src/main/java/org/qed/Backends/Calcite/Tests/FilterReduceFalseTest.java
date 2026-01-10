package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class FilterReduceFalseTest {

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
        
        // Before: scan + filter(false)
        var before = builder
            .push(scan)
            .filter(builder.literal(false))
            .build();
        
        // After: scan + empty()
        var after = builder
            .push(scan)
            .empty()
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.FilterReduceFalse.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running FilterReduceFalse test...");
        runTest();
    }
}