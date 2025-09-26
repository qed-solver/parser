package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;
import org.apache.calcite.rel.RelNode;

public class PruneEmptyMinusTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();

        var table = builder.createQedTable(
                Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false))
        );
        builder.addTable(table);

        RelNode scanA = builder
                .scan(table.getName())
                .build();

        RelNode scanB = builder
                .scan(table.getName())
                .build();

        RelNode before = builder
                .push(scanA)
                .push(scanB)
                .minus(false)
                .empty()
                .build();

        RelNode after = builder
                .push(scanA)
                .empty()
                .build();

//        var runner = CalciteTester.loadRule(
//                org.qed.Generated.PruneEmptyMinus.Config.DEFAULT.toRule()
//        );
//        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running PruneEmptyMinus test...");
        runTest();
    }
}
