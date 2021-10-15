package org.cosette;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Programs;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

public class RuleExtractor {

    private static final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
    private static final FrameworkConfig config = Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.Config.DEFAULT)
            .defaultSchema(rootSchema)
            .traitDefs((List<RelTraitDef>) null)
            .programs(Programs.heuristicJoinOrder(Programs.RULE_SET, true, 2)).build();
    private static final RelConstructor relBuilder = RelConstructor.create(config);

    public static void main(String[] args) throws IOException {
        for (RelOptRule rule : ruleList()) {
            extractRule(rule);
        }
    }

    public static List<RelOptRule> ruleList() {
        List<RelOptRule> list = new ArrayList<>();
        Field[] fields = CoreRules.class.getDeclaredFields();
        for (Field field : fields) {
            try {
                list.add((RelOptRule) field.get(RelOptRule.class));
            } catch (IllegalAccessException ignore) {

            }
        }
        return list;
    }

    public static void extractRule(RelOptRule rule) {
        buildPattern(rule.getOperand());
        RelNode original = relBuilder.build();
        HepProgram hepProgram = HepProgram.builder().addRuleInstance(rule).build();
        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        hepPlanner.setRoot(original);
        RelNode optimized = hepPlanner.findBestExp();
        if (!original.explain().equals(optimized.explain())) {
            System.out.println(rule);
        } else {
            System.out.println(rule.getOperand());
        }
//        List<RelNode> pair = new ArrayList<>(Arrays.asList(original, optimized));
//        StringBuilderWriter display = new StringBuilderWriter();
//        RelJSONShuttle.dumpToJSON(pair, display);
//        System.out.println(display);
    }

    public static void buildPattern(RelOptRuleOperand operand) {
        if (operand.getChildOperands().size() == 0) {
            relBuilder.var();
        } else {
            for (RelOptRuleOperand child : operand.getChildOperands()) {
                buildPattern(child);
            }
        }
        Class<? extends RelNode> type = operand.getMatchedClass();
        if (type.isAssignableFrom(Aggregate.class)) {

        } else if (type.isAssignableFrom(Values.class)) {

        } else if (type.isAssignableFrom(Filter.class)) {
            relBuilder.filter(new RexVariable());
        } else if (type.isAssignableFrom(Project.class)) {

        } else if (type.isAssignableFrom(Join.class)) {

        } else if (type.isAssignableFrom(Correlate.class)) {

        } else if (type.isAssignableFrom(Union.class)) {

        } else if (type.isAssignableFrom(Minus.class)) {

        } else if (type.isAssignableFrom(Sort.class)) {

        } else if (type.isAssignableFrom(RelNode.class)) {
            relBuilder.var();
        }
    }

}