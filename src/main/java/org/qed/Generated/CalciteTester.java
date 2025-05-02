package org.qed.Generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import kala.collection.Seq;
import kala.tuple.Tuple;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.qed.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;

public class CalciteTester {
    // Assuming that current working directory is the root of the project
    public static String genPath = "src/main/java/org/qed/Generated";
    public static String rulePath = "rules";

    public static HepPlanner loadRule(RelOptRule rule) {
        System.out.printf("Verifying Rule: %s\n", rule.getClass());
        var builder = new HepProgramBuilder().addRuleInstance(rule);
        return new HepPlanner(builder.build());
    }

    public static Seq<RRule> ruleList() {
        var individuals =
                Seq.from(RRuleInstance.class.getClasses()).filter(RRule.class::isAssignableFrom).mapUnchecked(Class::getConstructor).mapUnchecked(Constructor::newInstance).map(r -> (RRule) r);
        System.out.println(Seq.from(RRuleInstance.class.getClasses()).filter(RRule.RRuleFamily.class::isAssignableFrom).mapUnchecked(Class::getConstructor));
        /* To be restored */
        // var families =
        //         Seq.from(RRuleInstance.class.getClasses()).filter(RRule.RRuleFamily.class::isAssignableFrom).mapUnchecked(Class::getConstructor).mapUnchecked(Constructor::newInstance).map(r -> (RRule.RRuleFamily) r);
        // return individuals.appendedAll(families.flatMap(RRule.RRuleFamily::family));
        return individuals;
    }

    public static void verify() {
        ruleList().forEachUnchecked(rule -> rule.dump(rulePath + "/" + rule.name() + ".json"));
    }

    public static void generate() {
        var tester = new CalciteTester();
        ruleList().forEach(r -> tester.serialize(r, genPath));
    }

    public static void main(String[] args) throws IOException {
        // var rule = new RRuleInstance.FilterReduceTrue();
        // Files.createDirectories(Path.of(rulePath));
        // new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(rulePath, rule.name() + "-" + rule.info() + ".json").toFile(), rule.toJson());
        // var rules = new RRuleInstance.JoinAssociate();
        // Files.createDirectories(Path.of(rulePath));
        // for (var rule : rules.family()) {
        //     new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(rulePath, rule.name() + "-" + rule.info() + ".json").toFile(), rule.toJson());
        // }
        // generate();

        /* FilterIntoJoin */
        var tester = new CalciteTester();
        var builder = RuleBuilder.create();
        var table = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(table);
        var before = builder.scan(table.getName())
                .scan(table.getName())
                .join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("join", true), builder.joinFields()))
                .filter(builder.call(builder.genericPredicateOp("pred", true), builder.fields()))
                .build();
        var after = builder.scan(table.getName())
                .scan(table.getName())
                .join(JoinRelType.INNER, builder.call(SqlStdOperatorTable.AND,
                        builder.call(builder.genericPredicateOp("join", true), builder.joinFields()),
                        builder.call(builder.genericPredicateOp("pred", true), builder.joinFields())))
                .build();
        var runner = loadRule(FilterIntoJoin.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);

        /* FilterMerge */
        before = builder.scan(table.getName())
                .filter(builder.call(builder.genericPredicateOp("inner", true), builder.fields()))
                .filter(builder.call(builder.genericPredicateOp("outer", true), builder.fields()))
                .build();
        after = builder.scan(table.getName()).filter(builder.call(SqlStdOperatorTable.AND,
                        builder.call(builder.genericPredicateOp("inner", true), builder.fields()),
                        builder.call(builder.genericPredicateOp("outer", true), builder.fields())))
                .build();
        runner = loadRule(FilterMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);

        /* FilterProjectTranspose */
        builder = RuleBuilder.create();
        table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false),
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);
        var scan = builder.scan(table.getName()).build();
        before = builder
            .push(scan)
            .filter(builder.equals(builder.field(0), builder.literal(10)))
            .project(builder.field(0))
            .build();
        after = builder
            .push(scan)
            .project(builder.field(0))
            .filter(builder.equals(builder.field(0), builder.literal(10)))
            .build();
        runner = loadRule(FilterProjectTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);
        
        /* UnionMerge */
        builder = RuleBuilder.create();
        table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);
        var scan1 = builder.scan(table.getName()).build();
        var scan2 = builder.scan(table.getName()).build();
        var scan3 = builder.scan(table.getName()).build();
        var firstUnion = builder.push(scan1).push(scan2).union(false).build();
        before = builder.push(firstUnion).push(scan3).union(false).build();
        after = builder.push(scan1).push(scan2).push(scan3).union(false, 3).build();
        runner = loadRule(UnionMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);

        /* IntersectMerge */
        builder = RuleBuilder.create();
        table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);
        scan1 = builder.scan(table.getName()).build();
        scan2 = builder.scan(table.getName()).build();
        scan3 = builder.scan(table.getName()).build();   
        var firstIntersect = builder.push(scan1).push(scan2).intersect(false).build();
        before = builder.push(firstIntersect).push(scan3).intersect(false).build();
        after = builder.push(scan1).push(scan2).push(scan3).intersect(false, 3).build();
        runner = loadRule(IntersectMerge.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);

        /* FilterSetOpTranspose */
        builder = RuleBuilder.create();
        table = builder.createQedTable(Seq.of(
            Tuple.of(RelType.fromString("INTEGER", true), false)
        ));
        builder.addTable(table);
        scan1 = builder.scan(table.getName()).build();
        scan2 = builder.scan(table.getName()).build();
        var union = builder.push(scan1).push(scan2).union(false).build();
        before = builder.push(union).filter(builder.call(builder.genericPredicateOp("filter", true), builder.fields())).build();
        var filteredScan1 = builder.push(scan1).filter(builder.call(builder.genericPredicateOp("filter", true), builder.fields())).build();
        var filteredScan2 = builder.push(scan2).filter(builder.call(builder.genericPredicateOp("filter", true), builder.fields())).build();
        after = builder.push(filteredScan1).push(filteredScan2).union(false).build();
        runner = loadRule(FilterSetOpTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);

        /* JoinExtractFilter */
        builder = RuleBuilder.create();
        var leftTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        var rightTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("VARCHAR", true), false)));
        builder.addTable(leftTable);
        builder.addTable(rightTable);
        var leftScan = builder.scan(leftTable.getName()).build();
        var rightScan = builder.scan(rightTable.getName()).build();
        before = builder.push(leftScan).push(rightScan).join(JoinRelType.INNER, builder.call(builder.genericPredicateOp("join", true), builder.field(2, 0, 0), builder.field(2, 1, 0))).build();
        var trueJoin = builder.push(leftScan).push(rightScan).join(JoinRelType.INNER, builder.literal(true)).build();
        after = builder.push(trueJoin).filter(builder.call(builder.genericPredicateOp("join", true), builder.field(0), builder.field(1))).build();
        runner = loadRule(JoinExtractFilter.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);

        /* SemiJoinFilterTranspose */
        builder = RuleBuilder.create();
        leftTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        rightTable = builder.createQedTable(Seq.of(Tuple.of(RelType.fromString("INTEGER", true), false)));
        builder.addTable(leftTable);
        builder.addTable(rightTable);
        leftScan = builder.scan(leftTable.getName()).build();
        rightScan = builder.scan(rightTable.getName()).build();
        builder.push(leftScan);
        builder.push(rightScan);
        var joinPredicate = builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0));
        var semiJoin = builder.join(JoinRelType.SEMI, joinPredicate).build();
        builder.push(semiJoin);
        var filterPredicate = builder.call(builder.genericPredicateOp("filter", true), builder.field(0));
        before = builder.filter(filterPredicate).build();
        builder.push(leftScan);
        var leftFilterPredicate = builder.call(builder.genericPredicateOp("filter", true), builder.field(0));
        var filteredLeft = builder.filter(leftFilterPredicate).build();
        builder.push(filteredLeft);
        builder.push(rightScan);
        var afterJoinPredicate = builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0));
        after = builder.join(JoinRelType.SEMI, afterJoinPredicate).build();
        runner = loadRule(SemiJoinFilterTranspose.Config.DEFAULT.toRule());
        tester.verify(runner, before, after);

        /* JoinCommute */
        // TBD: failed
        // builder = RuleBuilder.create();
        // leftTable = builder.createQedTable(Seq.of(
        //     Tuple.of(RelType.fromString("INTEGER", true), false)
        // ));
        // rightTable = builder.createQedTable(Seq.of(
        //     Tuple.of(RelType.fromString("INTEGER", true), false)
        // ));
        // builder.addTable(leftTable);
        // builder.addTable(rightTable);
        // leftScan = builder.scan(leftTable.getName()).build();
        // rightScan = builder.scan(rightTable.getName()).build();
        // before = builder
        //     .push(leftScan)
        //     .push(rightScan)
        //     .join(
        //         JoinRelType.INNER,
        //         builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0))
        //     )
        //     .build();
        // after = builder
        //     .push(rightScan)
        //     .push(leftScan)
        //     .join(
        //         JoinRelType.INNER,
        //         builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0))
        //     )
        //     .build();
        // runner = loadRule(JoinCommute.Config.DEFAULT.toRule());
        // tester.verify(runner, before, after);

        /* ProjectMerge */
        // TBD: Automatically optimized?
        // builder = RuleBuilder.create();
        // table = builder.createQedTable(Seq.of(
        //     Tuple.of(RelType.fromString("INTEGER", true), false),
        //     Tuple.of(RelType.fromString("INTEGER", true), false),
        //     Tuple.of(RelType.fromString("INTEGER", true), false)  
        // ));
        // builder.addTable(table);
        // scan = builder.scan(table.getName()).build();
        // var innerProject = builder
        //     .push(scan)
        //     .project(builder.field(0), builder.field(1))
        //     .build();
        // before = builder
        //     .push(innerProject)
        //     .project(builder.field(0))
        //     .build();
        // after = builder
        //     .push(scan)
        //     .project(builder.field(0))
        //     .build();
        // runner = loadRule(ProjectMerge.Config.DEFAULT.toRule());
        // tester.verify(runner, before, after);

        /* JoinAddRedundantSemiJoin */ 
        // TBD: failed
        // builder = RuleBuilder.create();
        // leftTable = builder.createQedTable(Seq.of(
        //     Tuple.of(RelType.fromString("INTEGER", true), false)
        // ));
        // rightTable = builder.createQedTable(Seq.of(
        //     Tuple.of(RelType.fromString("INTEGER", true), false)
        // ));
        // builder.addTable(leftTable);
        // builder.addTable(rightTable);
        // leftScan = builder.scan(leftTable.getName()).build();
        // rightScan = builder.scan(rightTable.getName()).build();
        // before = builder
        //     .push(leftScan)
        //     .push(rightScan)
        //     .join(
        //         JoinRelType.INNER,
        //         builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0))
        //     )
        //     .build();
        // after = builder
        //     .push(leftScan)
        //     .push(rightScan)
        //     .join(JoinRelType.SEMI, builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0)))
        //     .push(rightScan)
        //     .join(JoinRelType.INNER, builder.equals(builder.field(2, 0, 0), builder.field(2, 1, 0)))
        //     .build();
        // runner = loadRule(JoinAddRedundantSemiJoin.Config.DEFAULT.toRule());
        // tester.verify(runner, before, after);

        /* FilterReduceFalse */
        // TBD: Automatically optimized?
        // builder = RuleBuilder.create();
        // table = builder.createQedTable(Seq.of(
        //     Tuple.of(RelType.fromString("INTEGER", true), false),
        //     Tuple.of(RelType.fromString("INTEGER", true), false)
        // ));
        // builder.addTable(table);
        // scan = builder.scan(table.getName()).build();
        // before = builder
        //     .push(scan)
        //     .filter(builder.equals(builder.field(0), builder.literal(10)))
        //     .filter(builder.literal(false))
        //     .build();
        // after = builder
        //     .push(scan)
        //     .empty()
        //     .build();
        // runner = loadRule(FilterReduceFalse.Config.DEFAULT.toRule());
        // tester.verify(runner, before, after);

        /* FilterReduceTrue */
        // TBD: Automatically optimized? And something wrong with this rule? 
        // builder = RuleBuilder.create();
        // table = builder.createQedTable(Seq.of(
        //     Tuple.of(RelType.fromString("INTEGER", true), false),
        //     Tuple.of(RelType.fromString("INTEGER", true), false)
        // ));
        // builder.addTable(table);
        // scan = builder.scan(table.getName()).build();
        // before = builder
        //     .push(scan)
        //     .filter(builder.equals(builder.field(0), builder.literal(10)))
        //     .filter(builder.literal(true)) 
        //     .filter(builder.equals(builder.field(1), builder.literal(20)))
        //     .build();
        // after = builder
        //     .push(scan)
        //     .filter(builder.equals(builder.field(0), builder.literal(10)))
        //     .filter(builder.equals(builder.field(1), builder.literal(20)))
        //     .build();
        // runner = loadRule(FilterReduceTrue.Config.DEFAULT.toRule());
        // tester.verify(runner, before, after);
    }

    public void serialize(RRule rule, String path) {
        var generator = new CalciteGenerator();
        var code_gen = generator.generate(rule);
        try {
            Files.write(Path.of(path, rule.name() + ".java"), code_gen.getBytes());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

    public void test(RelOptRule rule, Seq<String> tests) {
        System.out.println("Testing rule " + rule.getClass().getSimpleName());
        var runner = loadRule(rule);
        var exams = tests.mapUnchecked(t -> Tuple.of(t, JSONDeserializer.load(new File(t))));
        for (var entry : exams) {
            if (entry.getValue().size() != 2) {
                System.err.println(entry.getKey() + " does not have exactly two nodes, and thus is not a valid test");
                continue;
            }
            verify(runner, entry.getValue().get(0), entry.getValue().get(1));
        }
    }

    public void verify(HepPlanner runner, RelNode source, RelNode target) {
        runner.setRoot(source);
        var answer = runner.findBestExp();

        String answerExplain = answer.explain();
        String targetExplain = target.explain();
        
        if(answerExplain.equals(targetExplain)) {
            System.out.println("succeeded");
            // System.out.println("> Given source RelNode:\n" + source.explain());
            // System.out.println("> Actual rewritten RelNode:\n" + answerExplain);
            // System.out.println("> Expected rewritten RelNode:\n" + targetExplain);
            return;
        }
        System.out.println("failed");
        System.out.println("> Given source RelNode:\n" + source.explain());
        System.out.println("> Actual rewritten RelNode:\n" + answerExplain);
        System.out.println("> Expected rewritten RelNode:\n" + targetExplain);
    }
}
