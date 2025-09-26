package org.qed.Backends.Calcite.TrivialTests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RRuleInstances.JoinReduceFalse;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class JoinReduceFalseTest {

    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();

        var leftTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        var rightTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(leftTable);
        builder.addTable(rightTable);

        var before = builder.scan(leftTable.getName())
                .scan(rightTable.getName())
                .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.AND,
                        builder.call(builder.genericPredicateOp("join", true), builder.joinFields()),
                        builder.literal(false)))
                .build();

        var after = builder.scan(leftTable.getName())
                .scan(rightTable.getName())
                .join(JoinRelType.INNER, builder.literal(false))
                .build();

        var runner = CalciteTester.loadRule(org.qed.Generated.JoinReduceFalse.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running JoinReduceFalse test...");
        runTest();
    }
}