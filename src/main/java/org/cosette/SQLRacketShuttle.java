package org.cosette;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.IntPair;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * AN implementation of SqlShuttle interface that could convert a SqlNode instance to a ObjectNode instance.
 */
public class SQLRacketShuttle extends SqlShuttle {

//    private final Environment environment;
    private ArrayList<String> racketInput;

    /**
     * Initialize the shuttle with a given environment.
     *
     */
    public SQLRacketShuttle() {
        racketInput = new ArrayList<>();
    }

    /**
     * Dump a SqlNode to a writer in JSON format .
     *
     * @param sqlNode The given SQLNode.
     * @param writer   The given writer.
     */
    public static void dumpToRacket(SqlNode sqlNode, Writer writer) throws IOException {
        // writer.write(String);

        SQLRacketShuttle sqlRacketShuttle = new SQLRacketShuttle();
        sqlNode.accept(sqlRacketShuttle);

//        System.out.println(sqlNode.toString());
//        System.out.println();
//
//        SqlSelect select = (SqlSelect) sqlNode;
//        System.out.println(select.getGroup());

        // basic implementation
        // need to support SELECT (FROM, WHERE, GROUP BY, HAVING, WHERE), JOIN

        // more advanced
        // need to also support MERGE, ORDER BY, DELETE, DISTINCT (part of SELECT)

        // extra
        // FETCH, LIMIT, ORDER BY (all part of SELECT)
    }

    /**
     * @return The String corresponding to the Racket input for the SqlNode instance.
     */
    public String getRacketInput() {
//        return racketInput;
        return "";
    }


    // REQUIRED TO IMPLEMENT INTERFACE

    /**
     * Visits a call to a SqlOperator.
     * @param call
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlCall call) {
        System.out.println("SQL CALL");

        SqlKind sqlKind = call.getKind();

        switch (sqlKind) {
            case SELECT:
                System.out.println("\tSQL SELECT");

                // if there's a group by, string should have SELECT-GROUP
                // otherwise just SELECT
                SqlSelect sqlSelect = (SqlSelect) call;
//                racketInput

                System.out.println(((SqlSelect) call).getFrom());




//                call.accept(this);
                break;


            case JOIN:
                System.out.println("\tSQL JOIN");
                break;
        }


        return null;
    }

    /**
     * Visits a datatype specification.
     * @param type
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlDataTypeSpec type) {
        System.out.println("type");
        return null;
    }

    /**
     * Visits a dynamic parameter.
     * @param param
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlDynamicParam param) {
        System.out.println("param");
        return null;
    }

    /**
     * Visits an identifier.
     * @param id
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlIdentifier id) {
        System.out.println("id");
        return null;
    }

    /**
     * Visits an interval qualifier.
     * @param intervalQualifier
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
        System.out.println("interval qual");
        return null;
    }

    /**
     * Visits a literal.
     * @param literal
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlLiteral literal) {
        System.out.println("literal");
        return null;
    }

    /**
     * Visits a list of SqlNode objects.
     * @param nodeList
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlNodeList nodeList) {
        System.out.println("node list");
        return null;
    }
}
