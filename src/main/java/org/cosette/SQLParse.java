package org.cosette;

import org.apache.calcite.config.Lex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;

import java.util.Arrays;

public class SQLParse {
    public static void main( String[] args ) throws SqlParseException
    {
        String sql = """
                CREATE TABLE R(A INTEGER)""";
        SqlParser.Config sqlParserConfig = SqlParser.configBuilder()
                .setParserFactory(SqlDdlParserImpl.FACTORY)
                .setConformance(SqlConformanceEnum.MYSQL_5)
                .setLex(Lex.MYSQL)
                .build();

        SqlParser sqlParser = SqlParser.create(sql, sqlParserConfig);
        SqlNode sqlQuery = sqlParser.parseQuery();
        System.out.println(sqlQuery);
        FrameworkConfig config = Frameworks.newConfigBuilder().build();
        final RelBuilder builder = RelBuilder.create(config);
        final RelNode node = builder
                .scan("EMP")
                .aggregate(builder.groupKey("DEPTNO"),
                        builder.count(false, "C"),
                        builder.sum(false, "S", builder.field("SAL")))
                .filter(
                        builder.call(SqlStdOperatorTable.GREATER_THAN,
                                builder.field("C"),
                                builder.literal(10)))
                .build();
        node.explain(new RelJsonWriter());
    }
}

class SQLVisitorJSON implements SqlVisitor<Void> {
    @Override
    public Void visit(SqlLiteral sqlLiteral) {
        System.out.print(sqlLiteral.toValue());
        return null;
    }

    @Override
    public Void visit(SqlCall sqlCall) {
        if (sqlCall instanceof SqlSelect sqlSelect) {
            System.out.println("{\"select\": ");
            sqlSelect.getSelectList().accept(this);
            System.out.print(", \"from\": ");
            sqlSelect.getFrom().accept(this);
            System.out.println("}");
        } else if (sqlCall instanceof SqlBasicCall sqlBasicCall) {
            System.out.print(Arrays.toString(sqlBasicCall.getOperands()));
        } else if (sqlCall instanceof SqlJoin sqlJoin) {
            sqlJoin.getLeft().accept(this);
            System.out.print(" " + sqlJoin.getJoinType() + " ");
            sqlJoin.getCondition().accept(this);
            sqlJoin.getRight().accept(this);
        }
        return null;
    }

    @Override
    public Void visit(SqlNodeList sqlNodeList) {
        System.out.print("[");
        for (SqlNode sqlNode : sqlNodeList) {
            sqlNode.accept(this);
        }
        System.out.print("]");
        return null;
    }

    @Override
    public Void visit(SqlIdentifier sqlIdentifier) {
        System.out.print(sqlIdentifier.toString());
        return null;
    }

    @Override
    public Void visit(SqlDataTypeSpec sqlDataTypeSpec) {
        System.out.print(sqlDataTypeSpec.toString());
        return null;
    }

    @Override
    public Void visit(SqlDynamicParam sqlDynamicParam) {
        System.out.print(sqlDynamicParam.toString());
        return null;
    }

    @Override
    public Void visit(SqlIntervalQualifier sqlIntervalQualifier) {
        System.out.print(sqlIntervalQualifier.toString());
        return null;
    }
}
