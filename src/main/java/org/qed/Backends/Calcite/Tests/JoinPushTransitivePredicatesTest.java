package org.qed.Backends.Calcite.Tests;

import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.Backends.Calcite.CalciteTester;
import org.qed.RelType;
import org.qed.RuleBuilder;

public class JoinPushTransitivePredicatesTest {
    public static void runTest() {
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var table = builder.createQedTable(Seq.of(
                Tuple.of(RelType.fromString("INTEGER", true), false),
                Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);

        var before = builder.scan(table.getName())
                        .scan(table.getName()).join(JoinRelType.INNER,builder.call(builder.genericPredicateOp("cond1", true), builder.joinFields()))
                        .filter(builder.call(builder.genericPredicateOp("cond2", true), builder.fields()))
                        .build();

        var after = builder.scan(table.getName())
                        .scan(table.getName())
                                .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.AND,
                                        builder.call(builder.genericPredicateOp("cond1", true), builder.joinFields()),
                                        builder.call(builder.genericPredicateOp("cond2", true), builder.joinFields())))
                        .build();

        var runner = CalciteTester.loadRule(org.qed.Backends.Calcite.Generated.JoinPushTransitivePredicates.Config.DEFAULT.toRule());

        tester.verify(runner, before, after);
    }

    public static void main(String[] args) {
        System.out.println("Running ProjectFilterTranspose test...");
        runTest();
    }
}
