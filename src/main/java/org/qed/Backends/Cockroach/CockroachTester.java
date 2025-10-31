package org.qed.Backends.Cockroach;

import kala.tuple.Tuple;
import kala.collection.Seq;
import org.qed.*;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class CockroachTester {
    public static String genPath = "src/main/java/org/qed/Backends/Cockroach/Generated";
    public static String rulePath = "rules";

    public static HepPlanner loadRules(java.util.List<RelOptRule> rules) {
        System.out.printf("Loading Rules: %s\n",
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
        System.out.printf("Loading Rule: %s\n", rule.getClass().getSimpleName());
        var builder = new HepProgramBuilder().addRuleInstance(rule);
        return new HepPlanner(builder.build());
    }

    public static HepPlanner loadRule(RelOptRule rule, int matchLimit) {
        System.out.printf("Loading Rule: %s (match limit: %d)\n", rule.getClass().getSimpleName(), matchLimit);
        var builder = new HepProgramBuilder()
                .addMatchLimit(matchLimit)
                .addRuleInstance(rule);
        return new HepPlanner(builder.build());
    }

    public static Seq<RRule> ruleList() {
        java.io.File ruleDir = new java.io.File("src/main/java/org/qed/RRuleInstances");
        java.io.File[] files = ruleDir.listFiles((dir, name) -> name.endsWith(".java"));

        java.util.List<RRule> rules = new java.util.ArrayList<>();

        if (files != null) {
            for (java.io.File file : files) {
                String className = file.getName().replace(".java", "");
                if (className.contains("Distinct") || className.contains("Pull") ||
                    className.contains("ProjectAggregateMerge") ||
                    className.contains("AggregativeJoinRemove") || className.contains("AggregateProjectConstantToDummyJoin")) {
                    continue;
                }

                try {
                    Class<?> clazz = Class.forName("org.qed.RRuleInstances." + className);
                    RRule rule = (RRule) clazz.getConstructor().newInstance();
                    rules.add(rule);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to load rule: " + className, e);
                }
            }
        }

        return Seq.from(rules);

        // var families = Seq.from(reflections.getSubTypesOf(RRule.RRuleFamily.class))
        //         .filter(clazz -> !clazz.isInterface() && !Modifier.isAbstract(clazz.getModifiers()))
        //         .mapUnchecked(clazz -> {
        //             Constructor<? extends RRule.RRuleFamily> constructor = clazz.getDeclaredConstructor();
        //             constructor.setAccessible(true);
        //             return constructor.newInstance();
        //         })
        //         .map(r -> (RRule.RRuleFamily) r);

        // return individuals.appendedAll(families.flatMap(RRule.RRuleFamily::family));
    }

    public static void verify() {
        ruleList().forEachUnchecked(rule -> rule.dump(rulePath + "/" + rule.name() + ".json"));
    }

    public static void generate() {
        // Clean up any duplicate files with " 2.opt" or " 3.opt" suffixes (macOS/IDE artifacts)
        try {
            java.io.File genDir = new java.io.File(genPath);
            if (genDir.exists()) {
                java.io.File[] files = genDir.listFiles((dir, name) -> name.matches(".*\\s+[0-9]+\\.opt$"));
                if (files != null) {
                    for (java.io.File file : files) {
                        file.delete();
                    }
                }
            }
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        var tester = new CockroachTester();
        ruleList().forEach(r -> tester.serialize(r, genPath));
    }

    public static void runAllTests() {
        String packagePath = "src/main/java/org/qed/Backends/Cockroach/Tests";
        java.io.File testDir = new java.io.File(packagePath);
        java.io.File[] testFiles = testDir.listFiles((dir, name) -> name.endsWith("Test.java"));
        if (testFiles != null) {
            for (java.io.File testFile : testFiles) {
                String className = "org.qed.Backends.Calcite.Tests." + testFile.getName().replace(".java", "");
                try {
                    Class<?> testClass = Class.forName(className);
                    testClass.getMethod("runTest").invoke(null);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to run test: " + className, e);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException {
        // var rule = new org.qed.RRuleInstances.AggregateExtractProject();
        // System.out.println(rule.explain());
        // Files.createDirectories(Path.of(rulePath));
        // new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(rulePath, rule.name() + "-" + rule.info() + ".json").toFile(), rule.toJson());
        // var rules = new RRuleInstance.JoinAssociate();
        // Files.createDirectories(Path.of(rulePath));
        // for (var rule : rules.family()) {
        //     new ObjectMapper().writerWithDefaultPrettyPrinter().writeValue(Path.of(rulePath, rule.name() + "-" + rule.info() + ".json").toFile(), rule.toJson());
        // }
        generate();
        runAllTests();
    }

    public void serialize(RRule rule, String path) {
        var generator = new CockroachGenerator();
        var code_gen = generator.generate(rule);
        try {
            Files.write(Path.of(path, rule.name() + ".opt"), code_gen.getBytes());
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
            }
            return;
        }
        System.out.println("failed");
        System.out.println("> Given source RelNode:\n" + source.explain());
        System.out.println("> Actual rewritten RelNode:\n" + answerExplain);
        System.out.println("> Expected rewritten RelNode:\n" + targetExplain);
    }
}