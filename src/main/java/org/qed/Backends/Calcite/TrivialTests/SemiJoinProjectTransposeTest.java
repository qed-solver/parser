package org.qed.Backends.Calcite.TrivialTests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class SemiJoinProjectTransposeTest {
    public static void runTest() {
        var tester  = new CalciteTester();
        var builder = RuleBuilder.create();

        var leftTable  = builder.createQedTable(
                Seq.of(Tuple.of(RelType.fromString("INTEGER", true), /*nullable?*/ false)));
        var rightTable = builder.createQedTable(
                Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));

        builder.addTable(leftTable);
        builder.addTable(rightTable);

        var leftScan  = builder.scan(leftTable.getName()).build();
        var rightScan = builder.scan(rightTable.getName()).build();

        builder.push(leftScan);
        builder.push(rightScan);
        var joinPred  = builder.equals(
                builder.field(2, 0, 0),   // left field 0
                builder.field(2, 1, 0));  // right field 0
        var semiJoin  = builder.join(JoinRelType.SEMI, joinPred).build();
        builder.push(semiJoin);

        var before = builder.project(builder.field(0)).build();

        builder.push(leftScan);
        var projectedLeft = builder.project(builder.field(0)).build();

        builder.push(projectedLeft);
        builder.push(rightScan);
        var afterJoinPred = builder.equals(
                builder.field(2, 0, 0),
                builder.field(2, 1, 0));
        var after = builder.join(JoinRelType.SEMI, afterJoinPred).build();

        // var runner = CalciteTester.loadRule(
        //         org.qed.Generated.SemiJoinProjectTranspose.Config.DEFAULT.toRule());
        // tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running SemiJoinProjectTranspose test...");
        runTest();
    }
}
