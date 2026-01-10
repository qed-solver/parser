package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class UnionToDistinctTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create left and right tables with same schema
        var leftTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(leftTable);
        
        var rightTable = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(rightTable);
        
        var scanLeft = builder.scan(leftTable.getName()).build();
        var scanRight = builder.scan(rightTable.getName()).build();
        
        // Before: left UNION right (UNION DISTINCT, all=false)
        var before = builder
            .push(scanLeft)
            .push(scanRight)
            .union(false, 2)  // UNION DISTINCT
            .build();
        
        // After: (left UNION ALL right) then aggregate (group by all fields)
        var after = builder
            .push(scanLeft)
            .push(scanRight)
            .union(true, 2)  // UNION ALL
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(1))
            )
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.UnionToDistinct.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running UnionToDistinct test...");
        runTest();
    }
}