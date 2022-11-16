package org.cosette;

import kala.collection.Seq;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

public class ElevatedCoreRules {

    /**
     * Ignored rules:
     * - Aggregation related rules: unsupported for now
     *   - Aggregate*
     *   - CalcToWindow
     * - CalcRemove: trivially true
     * - CalcReduceDecimal: casting is not understood by the prover
     * - CalcReduceExpression: constant reduction is trivial
     * - CalcSplit: split calc into project above filter, which is exactly how calc is represented in cosette
     * - CoerceInputs: casting is not understood by the prover
     * - ExchangeRemoveConstantKeys: exchange not supported
     * - SortExchangeRemoveConstantKeys: exchange not supported
     */

    public static Tuple2<RelNode, RelNode> calcMerge() {
        // A Calc is equivalent to a project above a filter
        RuleBuilder builder = RuleBuilder.create();
        CosetteTable input = builder.createSimpleTable(Seq.of(Tuple.of(new RelType.VarType("INPUT", true), false)));
        SqlOperator bottomFilter = builder.constructGenericFunction("bottomFilter", new RelType.BaseType(SqlTypeName.BOOLEAN, true));
        SqlOperator bottomProject = builder.constructGenericFunction("bottomProject", new RelType.VarType("INTER", true));
        SqlOperator topFilter = builder.constructGenericFunction("topFilter", new RelType.BaseType(SqlTypeName.BOOLEAN, true));
        SqlOperator topProject = builder.constructGenericFunction("topProject", new RelType.VarType("RESULT", true));
        String cname = input.getColumnNames().get(0);
        builder.addTable(input);
        builder.scan(input.getName());
        builder.filter(builder.call(bottomFilter, builder.field(cname)));
        builder.project(List.of(builder.call(bottomProject, builder.field(cname))), List.of(cname));
        builder.filter(builder.call(topFilter, builder.field(cname)));
        builder.project(List.of(builder.call(topProject, builder.field(cname))), List.of(cname));
        RelNode before = builder.build();
        builder.scan(input.getName());
        builder.filter(builder.call(SqlStdOperatorTable.AND,
                builder.call(bottomFilter, builder.field(cname)),
                builder.call(topFilter, builder.call(bottomProject, builder.field(cname)))));
        builder.project(builder.call(topProject, builder.call(bottomProject, builder.field(cname))));
        RelNode after = builder.build();
        return Tuple.of(before, after);
    }

    public static void dumpElevatedRules(Path dumpFolder, boolean verbose) throws IOException {
        Files.createDirectories(dumpFolder);
        Seq.of(ElevatedCoreRules.class.getDeclaredMethods())
                .filter(method -> Modifier.isStatic(method.getModifiers()) && method.getReturnType().getName().equals("kala.tuple.Tuple2"))
                .forEachUnchecked(method -> {
                    String ruleName = method.getName();
                    Tuple2<RelNode, RelNode> rewrite = (Tuple2<RelNode, RelNode>) method.invoke(null);
                    if (verbose) {
                        System.out.println(">>>>>> " + ruleName + " <<<<<<");
                        System.out.println("Before:");
                        System.out.println(rewrite._1.explain());
                        System.out.println("After:");
                        System.out.println(rewrite._2.explain());
                    }
                    File dump = Paths.get(dumpFolder.toAbsolutePath().toString(), ruleName).toFile();
                    RelJSONShuttle.dumpToJSON(List.of(rewrite._1, rewrite._2), dump);
                });
    }

    public static void main(String[] args) throws IOException {
        Path dumpFolder = Paths.get("ElevatedRules");
        dumpElevatedRules(dumpFolder, true);
    }

}
