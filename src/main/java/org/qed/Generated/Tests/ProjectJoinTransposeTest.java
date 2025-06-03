package org.qed.Generated.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.Generated.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

/** Verifies ProjectJoinTranspose rule. */
public class ProjectJoinTransposeTest {

    public static void runTest() {
        var tester  = new CalciteTester();
        var builder = RuleBuilder.create();

        /* Two simple 2-column INTEGER tables */
        var leftTable  = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),
                Tuple.of(RelType.fromString("INTEGER", true), false)));
        var rightTable = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),
                Tuple.of(RelType.fromString("INTEGER", true), false)));

        builder.addTable(leftTable);
        builder.addTable(rightTable);

        var leftScan  = builder.scan(leftTable.getName()).build();
        var rightScan = builder.scan(rightTable.getName()).build();

        /* BEFORE:  ( Project(left) ) ⨝ right   — matches rule.before() */
        var before = builder
                .push(leftScan)
                .project(builder.field(0))  // trim left to a single column
                .push(rightScan)
                .join(JoinRelType.INNER)    // cartesian ON TRUE
                .build();

        /* AFTER:  Rule produces the same logical plan for this simple case */
        var after = builder
                .push(leftScan)
                .project(builder.field(0))
                .push(rightScan)
                .join(JoinRelType.INNER)
                .build();

        /* Load and verify ProjectJoinTranspose */
        var runner = CalciteTester.loadRule(
                org.qed.Generated.ProjectJoinTranspose.Config.DEFAULT.toRule());

        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running ProjectJoinTranspose test…");
        runTest();
    }
}
