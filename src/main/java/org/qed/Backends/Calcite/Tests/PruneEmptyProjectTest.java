package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class PruneEmptyProjectTest {

    public static void runTest() {
        var tester  = new CalciteTester();
        var builder = RuleBuilder.create();

        var table = builder.createQedTable(
                Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false))
        );
        builder.addTable(table);

        var before = builder
                .scan(table.getName())
                .empty()
                .project(builder.field(0))
                .build();

        var after = builder
                .scan(table.getName())
                .empty()
                .build();

        var runner = CalciteTester.loadRule(
                org.qed.Backends.Calcite.Generated.PruneEmptyProject.Config.DEFAULT.toRule()
        );
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running PruneEmptyProject test...");
        runTest();
    }
}
