package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class AggregateJoinJoinRemoveTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        
        // 简化：每个source table只有一个INTEGER字段
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
        
        var sourceC1 = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(sourceC1);
        
        var sourceC2 = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(sourceC2);
        
        // Build scans
        var scanA1 = builder.scan(sourceA1.getName()).build();
        var scanA2 = builder.scan(sourceA2.getName()).build();
        var scanB1 = builder.scan(sourceB1.getName()).build();
        var scanB2 = builder.scan(sourceB2.getName()).build();
        var scanC1 = builder.scan(sourceC1.getName()).build();
        var scanC2 = builder.scan(sourceC2.getName()).build();
        
        // Before: ((scanA1 JOIN scanA2) LEFT JOIN (scanB1 JOIN scanB2)) LEFT JOIN (scanC1 JOIN scanC2)
        // tblA: 2 fields [0,1]
        // tblB: 2 fields [2,3]
        // tblC: 2 fields [4,5]
        // After first LEFT JOIN (bottomJoin): 4 fields [0,1,2,3]
        // After second LEFT JOIN (topJoin): 6 fields [0,1,2,3,4,5]
        // Aggregate on field(0) from tblA and field(4) from tblC (NOT field(2) from tblB!)
        var before = builder
            .push(scanA1)
            .push(scanA2)
            .join(JoinRelType.INNER, builder.literal(true))
            .push(scanB1)
            .push(scanB2)
            .join(JoinRelType.INNER, builder.literal(true))
            .join(JoinRelType.LEFT, 
                builder.equals(
                    builder.field(2, 0, 0),  // Left (tblA), field 0
                    builder.field(2, 1, 0)   // Right (tblB), field 0
                )
            )
            .push(scanC1)
            .push(scanC2)
            .join(JoinRelType.INNER, builder.literal(true))
            .join(JoinRelType.LEFT,
                builder.equals(
                    builder.field(2, 0, 0),  // Left (bottomJoin), field 0 (tblA.field0)
                    builder.field(2, 1, 0)   // Right (tblC), field 0
                )
            )
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(4))  // tblA.f0 and tblC.f0
            )
            .build();
        
        // After: (scanA1 JOIN scanA2) LEFT JOIN (scanC1 JOIN scanC2)
        // tblA: 2 fields [0,1]
        // tblC: 2 fields [2,3]
        // After LEFT JOIN: 4 fields [0,1,2,3]
        // Aggregate on field(0) from tblA and field(2) from tblC
        var after = builder
            .push(scanA1)
            .push(scanA2)
            .join(JoinRelType.INNER, builder.literal(true))
            .push(scanC1)
            .push(scanC2)
            .join(JoinRelType.INNER, builder.literal(true))
            .join(JoinRelType.LEFT,
                builder.equals(
                    builder.field(2, 0, 0),  // Left (tblA), field 0
                    builder.field(2, 1, 0)   // Right (tblC), field 0
                )
            )
            .aggregate(
                builder.groupKey(builder.field(0), builder.field(2))  // tblA.f0 and tblC.f0
            )
            .build();
            
        var runner = CalciteTester.loadRule(
            org.qed.Backends.Calcite.Generated.AggregateJoinJoinRemove.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running AggregateJoinJoinRemove test...");
        runTest();
    }
}