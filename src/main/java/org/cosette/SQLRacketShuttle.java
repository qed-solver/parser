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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;

/**
 * AN implementation of SqlShuttle interface that could convert a SqlNode instance to a ObjectNode instance.
 */
public class SQLRacketShuttle extends SqlShuttle {

//    private final Environment environment;
    private static ArrayList<String> racketInput;
    private static boolean isFirstTable;
    private static int numTablesDefined;
    private static ArrayList<String> tableNames;

    // variable for one query
    // should re-initialize value after finishing performing one query.
    private static ArrayList<String> tableCols;
    private static boolean hasJoin;
    private static SqlNode whereForInnerJoin;
    private static String newJoinedTableName;
    private static Set<String> colNames;
    private static Map<String, String> colNameCoversion; // "TABLE.COLUMN" = "NEW_COLUMN_NAME"
    /**
     * Initialize the shuttle with a given environment.
     *
     */
    public SQLRacketShuttle() {
        racketInput = new ArrayList<>();
        isFirstTable = true;
        numTablesDefined = 0;
        tableNames = new ArrayList<>();


        tableCols = new ArrayList<>();
        hasJoin = false;
        newJoinedTableName = "";
        whereForInnerJoin = null;
        colNames = new HashSet<String>();
        colNameCoversion = new HashMap<String, String>();
    }

    /**
     * Dump a SqlNode to a writer in JSON format .
     *
     * @param sqlNodeList The given list of SQLNodes.
     * @param writer   The given writer.
     */
    public static void dumpToRacket(List<SqlNode> sqlNodeList, String ddl, Writer writer) throws IOException {
        // writer.write(String);

        SQLRacketShuttle sqlRacketShuttle = new SQLRacketShuttle();

        // Add required headers & modules.
        racketInput.add("#lang rosette\n\n");
        racketInput.add("(require \"../util.rkt\" \"../sql.rkt\" \"../table.rkt\"  \"../evaluator.rkt\" \"../equal.rkt\" \"../cosette.rkt\")\n\n");

        racketInput.add(helpFormatDDL(ddl));

        for (SqlNode sqlNode : sqlNodeList) {
            sqlNode.accept(sqlRacketShuttle);

            // Add line spacing for next statement.
            racketInput.add("\n\n");

            isFirstTable = false;
            tableCols = new ArrayList<>();
            hasJoin = false;
            newJoinedTableName = "";
            whereForInnerJoin = null;
        }

        // basic implementation
        // need to support SELECT (FROM, WHERE, GROUP BY, HAVING, WHERE), JOIN

        // more advanced
        // need to also support MERGE, ORDER BY, DELETE, DISTINCT (part of SELECT)

        // extra
        // FETCH, LIMIT, ORDER BY (all part of SELECT)

        // add code to run Rosette counterexample engine
        // THIS DEPENDS ON THE NUMBER OF TABLES BEING REFERENCED!!!
        racketInput.add(helpFormatRun(numTablesDefined));

        System.out.println("racket input");
        System.out.println(String.join("", racketInput));

        writer.write(String.join("", racketInput));
    }

    /**
     * @return The String corresponding to the Racket input for the SqlNode instance.
     */
    public String getRacketInput() {
        return String.join("", racketInput);
    }

    /**
     * Formats Rosette run call for given racket file.
     * @param numTables
     * @return String, racket formatted from clause.
     */
    private static String helpFormatRun(int numTables) {
        String runCall = "\n(let* ([model (verify (same q1s q2s))]\n";

        for (int i = 0; i < numTables; i++) {
            runCall = runCall + "\t   [concrete-t" + (i + 1) + " (clean-ret-table (evaluate " + tableNames.get(i) + " model))]";
            if (i != numTables - 1) {
                runCall = runCall + "\n";
            }
        }
        runCall = runCall + ")\n";

        for (int i = 0; i < numTables; i++) {
            runCall = runCall + "\t(println concrete-t" + (i + 1) + ")\n";
        }
        runCall = runCall + ")";

        return runCall;
    }

    /**
     * Formats CREATE_TABLE for the aliased table extracted from using DDL.
     * @param ddl
     * @return String, racket formatted from clause.
     */
    private static String helpFormatDDL(String ddl) {
        // separate unique DDL statements
        String[] ddlStatements = ddl.split("<END_TOKEN>");

        ArrayList<String> toReturnArr = new ArrayList<>();
        String toReturn = "";

        for (String d : ddlStatements) {
            // strip newlines, tabs, 4 spaces used for indentation
            // change to uppercase then get array
            String[] ddlWords = d.replaceAll("\t", "")
                    .replaceAll("\n", "")
                    .replaceAll("    ", " ")
                    .toUpperCase().split(" ");

            boolean addedCreateTable = false;
            String tableName = "";
            int numCols = 0;

            for (int i = 1; i < ddlWords.length; i++) {
//            System.out.println(ddlWords[i - 1]);
                String word = ddlWords[i - 1];
                String word2 = ddlWords[i];

                if (word.equals("CREATE")) {
                    numTablesDefined++;
                    toReturnArr.add("(define ");
                }
                if (word.equals("TABLE")) {
                    tableName = word2.replace("(", "");
                    tableNames.add(tableName);
                    toReturnArr.add(tableName);
                    toReturnArr.add(" (Table ");
                    toReturnArr.add("\"" + tableName + "\"");
                    toReturnArr.add(" (list");
                    addedCreateTable = true;
                    continue;
                }

                if (word2.equals(")")) {
                    toReturnArr.add(") (gen-sym-schema " + numCols + " 1)))");
                    toReturnArr.add("\n");
                    break;
                }

                if (addedCreateTable) {
                    numCols++;
                    i++;

                    // Check name duplication for two tables
                    String colName = word2.trim();
                    if (colNames.contains(colName)) {
                        // There's name duplication between two tables
                        // so we need to rename one of colname
                        String originalCol = tableNames.get(tableNames.size()-1) + "." + colName;
                        colName = colName + "0";
                        colNameCoversion.put(originalCol, colName);
                    }
                    colNames.add(colName);
                    toReturnArr.add(" \"" + colName + "\"");
                }
            }
        }

        toReturnArr.add("\n\n");

        for (String r : toReturnArr) {
            toReturn = toReturn + r;
        }

        return toReturn;
    }


    /**
     * Formats a string for the aliased table extracted from a SQL FROM clause.
     * @param from
     * @return String, racket formatted from clause.
     */
    private String helpFormatFrom(String from) {
        String[] fromWords = from.split(" ");
        String toReturn = " ";

        for (String word : fromWords) {
            if (word.equals("AS")) {
                break;
            }
            word = word.replaceAll("`", "");
            toReturn = toReturn + word;
        }
        return toReturn;
    }

    private String helpFormatWhereClause(String operand) {
        if (operand.charAt(0) == '`') {
            // `INDIV_SAMPLE_NYC`.`CMTE_ID` => "INDIV_SAMPLE_NYC.CMTE_ID"
            operand = operand.replaceAll("`", "");
            if (newJoinedTableName != "") {
                String[] operandTokens = operand.split("\\.");
                String colName = operandTokens[1];
                // Check whether name conversion is required
                if (colNameCoversion.containsKey(operand)) {
                    colName = colNameCoversion.get(operand);
                }
                // We have generated new joined table, so we need to change all table names to new joined table name

                return String.format("\"%s.%s\"", newJoinedTableName, colName);
            }
            return "\"" + operand + "\"";
        } else return operand;
    }

    private void helpFormatWhere(SqlNode where) {
        SqlKind comparisonType = where.getKind();
        switch (comparisonType) {
            case LESS_THAN:
            case GREATER_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN_OR_EQUAL:
            case EQUALS: {
                String[] whereTokens = where.toString().split(" ");
                whereTokens[0] = helpFormatWhereClause(whereTokens[0]);
                whereTokens[2] = helpFormatWhereClause(whereTokens[2]);
                racketInput.add(" (BINOP " + String.join(" ", whereTokens) + ")");
                break;
            }
            case OR:
            case AND: {
                // For OR and AND Operand, (sqlNode)where consists of "(BINOP WHERE) + (OR || AND) + (BINOP WHERE)"
                // Thus, we need to recursively parse where clause
                List<SqlNode> whereOperands = ((SqlBasicCall) where).getOperandList();
                racketInput.add(" (" + comparisonType);
                helpFormatWhere(whereOperands.get(0));
                helpFormatWhere(whereOperands.get(1));
                racketInput.add(" )");
                break;
            }
            case NOT_EQUALS: {
                // For queries like "WHERE id <> 3",  (Calcite library hasn't support "!=" yet)
                // transform it to racket format as "WHERE (NOT (BINOP id = 3))"
                String[] whereTokens = where.toString().split(" ");
                whereTokens[0] = helpFormatWhereClause(whereTokens[0]);
                whereTokens[1] = "=";
                whereTokens[2] = helpFormatWhereClause(whereTokens[2]);
                racketInput.add(" (NOT (BINOP " + String.join(" ", whereTokens) + ") )");
                break;
            }
        }
    }

    private String[] helperGetJoinTables(SqlJoin join) {
        // idx 0 -> Left Table Name, idx 1 -> Right Table Name
        String[] output = new String[2];
        output[0] = join.getLeft().toString().split(" ")[0].replaceAll("`", "");
        output[1] = join.getRight().toString().split(" ")[0].replaceAll("`", "");
        return output;
    }

    // REQUIRED TO IMPLEMENT INTERFACE
    /**
     * Visits a call to a SqlOperator.
     * @param call
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlCall call) {
//        System.out.println("SQL CALL");

        SqlKind sqlKind = call.getKind();

        switch (sqlKind) {
            case AS:
                System.out.println("\tSQL AS\n");
                System.out.println(call.toString());
                // Handle name conversion if there're duplicated column name
                String identifier = call.toString().split(" ")[0]; // `SF`.`CMTE_ID`
                identifier = identifier.replaceAll("`", "");
                if (colNameCoversion.containsKey(identifier)) {
                    String newColName = colNameCoversion.get(identifier);
                    racketInput.add(" \"" + newJoinedTableName + "." + newColName + "\"");
                    tableCols.add(newColName);
                }
                break;
            case JOIN: {
                System.out.println("\tSQL JOIN\n");
                SqlJoin sqlJoin = (SqlJoin) call;
                String joinType = sqlJoin.getJoinType().toString();
                racketInput.add(" FROM (AS ");

                // TODO: Handle table renamed issue: FROM table_a TA JOIN table_b TB => TA & TB renaming part
                String[] tableNames = helperGetJoinTables(sqlJoin);

                switch (joinType) {
                    case "RIGHT":
                    case "LEFT": {
                        racketInput.add("(LEFT-OUTER-JOIN ");

                        String firstTable = "", secondTable = "";
                        if (joinType == "LEFT") {
                            firstTable = tableNames[0];
                            secondTable = tableNames[1];
                        } else {
                            firstTable = tableNames[1];
                            secondTable = tableNames[0];
                        }

                        racketInput.add("(NAMED " + firstTable + ") (NAMED " + secondTable + ")");
                        helpFormatWhere(sqlJoin.getCondition());
                        racketInput.add(") ");

                        // Process "AS (JOINED_PART) "NEW_TABLE""
                        // ["table" (list "x_uid" "x_uname" "x_city" "y_uid" "y_size")]
                        racketInput.add("[ \"" + newJoinedTableName + "\"");
                        racketInput.add(" (list");
                        for (String tableCol: tableCols) {
                            racketInput.add(" \"" + tableCol + "\"");
                        }
                        racketInput.add(") ])");
                        break;
                    }
                    case "INNER":
                        // 1. Join two tables
                        racketInput.add("(JOIN ");
                        String firstTable = tableNames[0], secondTable = tableNames[1];
                        racketInput.add("(NAMED " + firstTable + ") (NAMED " + secondTable + ")) ");

                        // 2. Name combined table and cols
                        racketInput.add("[ \"" + newJoinedTableName + "\"");
                        racketInput.add(" (list");
                        for (String tableCol : tableCols) {
                            racketInput.add(" \"" + tableCol + "\"");
                        }
                        racketInput.add(") ])");

                        whereForInnerJoin = sqlJoin.getCondition();
                        break;
                    case "FULL":
                        // TODO: Need Implement
                        break;
                }

                break;
            }
            case SELECT:
                System.out.println("\tSQL SELECT\n");
                racketInput.add("(");

                if (isFirstTable) {
                    racketInput.add("define q1s (");
                } else {
                    racketInput.add("define q2s (");
                }

                racketInput.add("SELECT");

                // if there's a group by, string should have SELECT-GROUP
                // otherwise just SELECT
                SqlSelect sqlSelect = (SqlSelect) call;

                // Check whether statement involves "JOIN"
                // If involved, we should transform table name to new joined table name
                // We should check here because we need change the table name of selected cols to new joined table name
                SqlNode from = sqlSelect.getFrom();
                if (from.getKind().equals(sqlKind.JOIN)) {
                    hasJoin = true;
                    String[] tableNames = helperGetJoinTables((SqlJoin) from);
                    newJoinedTableName = tableNames[0] + "_JOIN_" + tableNames[1];
                }

                List<SqlNode> selectList = sqlSelect.getSelectList();
                if (!selectList.isEmpty()) {
                    racketInput.add(" (VALS");
                }

                for (SqlNode select : selectList) {
                    select.accept(this);
                }
                racketInput.add(")");

                if (hasJoin) from.accept(this);
                else if (from != null) {
                    racketInput.add(" FROM");

                    racketInput.add((" (NAMED"));
                    racketInput.add(helpFormatFrom(from.toString()));
                    racketInput.add(")");
                }

                SqlNode where = sqlSelect.getWhere();
                if (where == null && whereForInnerJoin == null) {
                    racketInput.add(" WHERE (TRUE)");
                } else if (where != null && whereForInnerJoin != null) {
                    racketInput.add(" WHERE (AND");
                    helpFormatWhere(whereForInnerJoin);
                    helpFormatWhere(where);
                    racketInput.add(") ");
                } else {
                    // Either "where" is null or "whereForInnerJoin" is null
                    racketInput.add(" WHERE");
                    if (where != null) helpFormatWhere(where);
                    else helpFormatWhere(whereForInnerJoin);
                }

                racketInput.add("))");
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
        System.out.println("ID\n");
        // e.g. "INDIV_SAMPLE_NYC.CMTE_ID"

        if (!hasJoin) {
            racketInput.add(" \"" + id.toString() + "\"");
        } else {
            String[] splitColName = id.toString().split("\\.");
            racketInput.add(" \"" + newJoinedTableName + "." + splitColName[1] + "\"");
            tableCols.add(splitColName[1]);
        }

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
