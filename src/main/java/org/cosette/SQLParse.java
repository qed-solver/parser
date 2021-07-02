package org.cosette;

import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.server.ServerDdlExecutor;
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
import org.apache.calcite.tools.ValidationException;

import java.io.StringReader;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;

public class SQLParse {

    public static void main( String[] args )
            throws ClassNotFoundException, SQLException, SqlParseException, ValidationException
    {
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        info.setProperty(CalciteConnectionProperty.PARSER_FACTORY.camelName(), ServerDdlExecutor.class.getName() + "#PARSER_FACTORY");
        info.setProperty(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED.camelName(), "true");
        info.setProperty(CalciteConnectionProperty.FUN.camelName(), "standard,oracle");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        Statement statement = calciteConnection.createStatement();

        statement.execute("""
                              CREATE TABLE EMP (
                             	EMP_ID INTEGER,
                             	EMP_NAME CHARACTER,
                             	DEPT_ID INTEGER
                             )
                             """);

        statement.execute("""
                            CREATE TABLE DEPT (
                                DEPT_ID INTEGER,
                                DEPT_NAME CHARACTER 
                            )
                            """);

        ResultSet result = statement.executeQuery("""
                            SELECT * FROM
                            (SELECT * FROM EMP WHERE DEPT_ID = 10) AS T
                            WHERE T.DEPT_ID + 5 > T.EMP_ID
                            """);

        statement.close();

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
