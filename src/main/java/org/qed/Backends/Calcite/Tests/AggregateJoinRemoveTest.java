package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class AggregateJoinRemoveTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // 所有字段都用INTEGER，避免VARCHAR charset问题
        var sourceA1 = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(sourceA1);
        
        var sourceA2 = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(sourceA2);
        
        var sourceB1 = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(sourceB1);
        
        var sourceB2 = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(sourceB2);
        
        // Build scans
        var scanA1 = builder.scan(sourceA1.getName()).build();
        var scanA2 = builder.scan(sourceA2.getName()).build();
        var scanB1 = builder.scan(sourceB1.getName()).build();
        var scanB2 = builder.scan(sourceB2.getName()).build();
        
        // Before: (scanA1 JOIN scanA2) LEFT JOIN (scanB1 JOIN scanB2), then aggregate
        var before = builder
            .push(scanA1)
            .push(scanA2)
            .join(JoinRelType.INNER, builder.literal(true))
            .push(scanB1)
            .push(scanB2)
            .join(JoinRelType.INNER, builder.literal(true))
            .join(JoinRelType.LEFT, 
                builder.equals(
                    builder.field(2, 0, 0),
                    builder.field(2, 1, 0)
                )
            )
            .aggregate(
                builder.groupKey(builder.field(0))
            )
            .build();
        
        // After: just (scanA1 JOIN scanA2) then aggregate
        var after = builder
            .push(scanA1)
            .push(scanA2)
            .join(JoinRelType.INNER, builder.literal(true))
            .aggregate(
                builder.groupKey(builder.field(0))
            )
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.AggregateJoinRemove.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running AggregateJoinRemove test...");
        runTest();
    }
}