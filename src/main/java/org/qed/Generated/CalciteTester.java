package org.qed.Generated;

import com.fasterxml.jackson.databind.ObjectMapper;
import kala.collection.Seq;
import kala.tuple.Tuple;

import org.apache.calcite.jdbc.CalcitePrepare.SparkHandler.RuleSetBuilder;
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
import org.reflections.Reflections;
import org.apache.calcite.rel.rules.*;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

public class CalciteTester {
    // Assuming that current working directory is the root of the project
    public static String genPath = "src/main/java/org/qed/Generated";
    public static String rulePath = "rules";

    public static HepPlanner loadRules(java.util.List<RelOptRule> rules) {
        System.out.printf("Verifying Rules: %s\n", 
            rules.stream()
                .map(rule -> rule.getClass().getSimpleName())
                .collect(java.util.stream.Collectors.joining(", ")));
        
        var builder = new HepProgramBuilder();
        for (var rule : rules) {
            builder.addRuleInstance(rule);
        }
        return new HepPlanner(builder.build());
    }

    public static HepPlanner loadRules(RelOptRule... rules) {
        return loadRules(java.util.Arrays.asList(rules));
    }

    public static HepPlanner loadRule(RelOptRule rule) {
        System.out.printf("Verifying Rule: %s\n", rule.getClass());
        var builder = new HepProgramBuilder().addRuleInstance(rule);
        return new HepPlanner(builder.build());
    }

    public static HepPlanner loadRule(RelOptRule rule, int matchLimit) {
        System.out.printf("Verifying Rule: %s (match limit: %d)\n", rule.getClass(), matchLimit);
        var builder = new HepProgramBuilder()
            .addMatchLimit(matchLimit)
            .addRuleInstance(rule);
        return new HepPlanner(builder.build());
    }

    public static Seq<RRule> ruleList() {
        Reflections reflections = new Reflections("org.qed.Generated.RRuleInstances");
    
        Set<Class<? extends RRule>> ruleClasses = reflections.getSubTypesOf(RRule.class);
        var concreteRuleClasses = ruleClasses.stream()
                .filter(clazz -> !clazz.isInterface() && 
                            !Modifier.isAbstract(clazz.getModifiers()) && 
                            !clazz.getName().contains("$") 
                            && !clazz.getSimpleName().equals("UnionPullUpConstants")
                            && !clazz.getSimpleName().equals("AggregateProjectConstantToDummyJoin")
                            && !clazz.getSimpleName().equals("ProjectAggregateMerge")
                            && !clazz.getSimpleName().equals("UnionToDistinct")
                        )
                .collect(Collectors.toSet());
        
        var individuals = Seq.from(concreteRuleClasses)
                .mapUnchecked(Class::getConstructor)
                .mapUnchecked(Constructor::newInstance)
                .map(r -> (RRule) r);
        
        // var families = Seq.from(reflections.getSubTypesOf(RRule.RRuleFamily.class))
        //         .filter(clazz -> !clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()))
        //         .mapUnchecked(clazz -> {
        //             Constructor<? extends RRule.RRuleFamily> constructor = clazz.getDeclaredConstructor();
        //             constructor.setAccessible(true);
        //             return constructor.newInstance();
        //         })
        //         .map(r -> (RRule.RRuleFamily) r);
        
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

    public static void runAllTests() {
        try {
            // org.qed.Generated.Tests.FilterIntoJoinTest.runTest();
            // org.qed.Generated.Tests.FilterMergeTest.runTest();
            // org.qed.Generated.Tests.FilterProjectTransposeTest.runTest();
            // org.qed.Generated.Tests.UnionMergeTest.runTest();
            // org.qed.Generated.Tests.IntersectMergeTest.runTest();
            // org.qed.Generated.Tests.FilterSetOpTransposeTest.runTest();
            // org.qed.Generated.Tests.JoinExtractFilterTest.runTest();
            // org.qed.Generated.Tests.SemiJoinFilterTransposeTest.runTest();
            // org.qed.Generated.Tests.MinusMergeTest.runTest();
            // org.qed.Generated.Tests.ProjectFilterTransposeTest.runTest();
            // org.qed.Generated.Tests.JoinPushTransitivePredicatesTest.runTest();
            // org.qed.Generated.Tests.JoinCommuteTest.runTest();
            // org.qed.Generated.Tests.JoinConditionPushTest.runTest();
            // org.qed.Generated.Tests.AggregateProjectMergeTest.runTest();
            // org.qed.Generated.Tests.AggregateFilterTransposeTest.runTest();
            // org.qed.Generated.Tests.FilterAggregateTransposeTest.runTest();
            org.qed.Generated.Tests.AggregateExtractProjectTest.runTest();
        } catch (Exception e) {
            System.out.println("Test failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        var rule = new org.qed.Generated.RRuleInstances.AggregateExtractProject();
        System.out.println(rule.explain());
        Files.createDirectories(Path.of(rulePath));
        new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(rulePath, rule.name() + "-" + rule.info() + ".json").toFile(), rule.toJson());
        // var rules = new RRuleInstance.JoinAssociate();
        // Files.createDirectories(Path.of(rulePath));
        // for (var rule : rules.family()) {
        //     new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(rulePath, rule.name() + "-" + rule.info() + ".json").toFile(), rule.toJson());
        // }
        generate();
        runAllTests();
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
            if(answerExplain.equals(source.explain()))
            {
                System.out.println("trivial");
                System.out.println("> Given source RelNode:\n" + source.explain());
                System.out.println("> Actual rewritten RelNode:\n" + answerExplain);
                System.out.println("> Expected rewritten RelNode:\n" + targetExplain);
            }
            else 
            {
                System.out.println("succeeded");
                // System.out.println("> Given source RelNode:\n" + source.explain());
                // System.out.println("> Actual rewritten RelNode:\n" + answerExplain);
                // System.out.println("> Expected rewritten RelNode:\n" + targetExplain);
            }
            return;
        }
        System.out.println("failed");
        System.out.println("> Given source RelNode:\n" + source.explain());
        System.out.println("> Actual rewritten RelNode:\n" + answerExplain);
        System.out.println("> Expected rewritten RelNode:\n" + targetExplain);
    }
}
