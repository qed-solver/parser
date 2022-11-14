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
import java.util.List;

public class ElevatedCoreRules {

    /**
     * Ignored rules:
     * - Aggregation related rules
     */

    public static Tuple2<RelNode, RelNode> calcMerge() {
        // A Calc is equivalent to a project above a filter
        RuleBuilder builder = RuleBuilder.create();
        CosetteTable input = builder.createSimpleTable(Seq.of(Tuple.of(new RelType.VarType("INPUT", true), false)));
        SqlOperator bottomProject = builder.constructGenericFunction("bottomProject", new RelType.VarType("INTER", true));
        SqlOperator bottomFilter = builder.constructGenericFunction("bottomFilter", new RelType.BaseType(SqlTypeName.BOOLEAN, true));
        SqlOperator topProject = builder.constructGenericFunction("topProject", new RelType.VarType("RESULT", true));
        SqlOperator topFilter = builder.constructGenericFunction("topFilter", new RelType.BaseType(SqlTypeName.BOOLEAN, true));
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

    public static void main(String[] args) throws IOException {
        Tuple2<RelNode, RelNode> rewrite = ElevatedCoreRules.calcMerge();
        System.out.println("Before:");
        System.out.println(rewrite._1.explain());
        System.out.println("After:");
        System.out.println(rewrite._2.explain());
        File dump = new File("CalciteRewriteRule.json");
        RelJSONShuttle.dumpToJSON(List.of(rewrite._1, rewrite._2), dump);
    }

}
