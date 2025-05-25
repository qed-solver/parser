package org.qed.Generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.qed.*;
import org.qed.RRuleInstance.SemiJoinRemove;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;


public class PipelineInstance {
    static final String rulePath = "rules";
    static final String genPath = "src/main/java/org/qed/Generated";
    public static void runTest() {
        var rule = new RRuleInstance.SemiJoinRemove();
        var builder = RuleBuilder.create();
        var tester = new CalciteTester();
        
        var table = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(table);
        
        var before = builder
                .scan(table.getName())
                .scan(table.getName())
                .join(JoinRelType.SEMI, builder.literal(true))
                .build();
        var after = builder
                .scan(table.getName())
                .build();

                
        // tester.serialize(rule, genPath);
        var runner = tester.loadRule(org.qed.Generated.SemiJoinRemove.Config.DEFAULT.toRule());
        System.out.println("verifying:");
        tester.verify(runner, before, after);
    }
    
    public static void main(String[] args) throws IOException {
        runTest();
    }
}