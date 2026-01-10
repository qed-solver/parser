package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class JoinAddRedundantSemiJoinTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // Create left and right tables
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
        
        // Before: left INNER JOIN right ON left.f0 = right.f0
        var before = builder
            .push(scanLeft)
            .push(scanRight)
            .join(JoinRelType.INNER,
                builder.equals(
                    builder.field(2, 0, 0),  // left.f0
                    builder.field(2, 1, 0)   // right.f0
                )
            )
            .build();
        
        // After: (left SEMI JOIN right ON left.f0 = right.f0) INNER JOIN right ON left.f0 = right.f0
        var after = builder
            .push(scanLeft)
            .push(scanRight)
            .join(JoinRelType.SEMI,
                builder.equals(
                    builder.field(2, 0, 0),  // left.f0
                    builder.field(2, 1, 0)   // right.f0
                )
            )
            .push(scanRight)
            .join(JoinRelType.INNER,
                builder.equals(
                    builder.field(2, 0, 0),  // left.f0 (from semi join result)
                    builder.field(2, 1, 0)   // right.f0 (from new right scan)
                )
            )
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.JoinAddRedundantSemiJoin.Config.DEFAULT.toRule(), 1
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running JoinAddRedundantSemiJoin test...");
        runTest();
    }
}