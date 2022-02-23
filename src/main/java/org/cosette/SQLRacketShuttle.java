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
import java.lang.reflect.Array;
import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.Stack;

/**
 * AN implementation of SqlShuttle interface that could convert a SqlNode instance to a ObjectNode instance.
 */
public class SQLRacketShuttle extends SqlShuttle {

//    private final Environment environment;
    private static ArrayList<String> racketInput;
    // jarrett
    private static ArrayList<String> racketInputBuffer;
    private static Stack<ArrayList<String>> aggrStack;
    private static boolean isFirstTable;
    private static int numTablesDefined;
    private static ArrayList<String> tableNames;
    private static boolean likeRegexMatches;
    private static ArrayList<ArrayList<List<SqlNode>>> likeRegex;
    private static int likeRegexTableIdx;
    private static ArrayList<String> symbolicVals;
    private static int likeRegexIdx;
    private static HashMap<String, String> likeRegexToSymbolicVal;
    private static boolean orderByMatches;
    private static ArrayList<SqlNode> table1OrderBy;

    /**
     * Initialize the shuttle with a given environment.
     *
     */
    public SQLRacketShuttle() {
        racketInput = new ArrayList<>();
        // jarrett
        racketInputBuffer = new ArrayList<>();
        aggrStack = new Stack<ArrayList<String>>();

        isFirstTable = true;
        numTablesDefined = 0;
        tableNames = new ArrayList<>();
        likeRegexMatches = true;
        likeRegex = new ArrayList<>();
        likeRegexTableIdx = -1;
        symbolicVals = new ArrayList<>();
        likeRegexIdx = 0;
        likeRegexToSymbolicVal = new HashMap<>();
        orderByMatches = true;
        table1OrderBy = new ArrayList<>();
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

        for (SqlNode sqlNode : sqlNodeList) {
            likeRegex.add(new ArrayList<>());
            likeRegexTableIdx++;
            sqlNode.accept(sqlRacketShuttle);

            // Add line spacing for next statement.
            racketInput.add("\n\n");

            isFirstTable = false;
        }

        // longest chain of LIKE clauses = num of symbolic bools to define
        int numSymbolicBools = 0;
        for (ArrayList<List<SqlNode>> s : likeRegex) {
            numSymbolicBools = numSymbolicBools + s.size();
            if (s.size() != numSymbolicBools) {
                likeRegexMatches = false;
            }
        }

        racketInput = new ArrayList<>();
        likeRegexTableIdx = -1;
        isFirstTable = true;

        // Add required headers & modules.
        racketInput.add("#lang rosette\n\n");
        racketInput.add("(require \"../util.rkt\" \"../sql.rkt\" \"../table.rkt\"  \"../evaluator.rkt\" \"../equal.rkt\" \"../cosette.rkt\" \"../denotation.rkt\" \"../syntax.rkt\")\n\n");

        racketInput.add(helpFormatDDL(ddl));

        char name = 'a';
        for (int i = 0; i < numSymbolicBools; i++) {
            // define symbolic values
            racketInput.add("(define (gen-" + name + ")\n");
            racketInput.add("\t(define-symbolic* " + name + " boolean?)\n");
            racketInput.add("  " + name + ")\n");
            symbolicVals.add(Character.toString(name));
            name++;
        }
        racketInput.add("\n");

        // second pass through nodes, to replace matching pairs with symbolic boolean values
        for (SqlNode sqlNode : sqlNodeList) {
            likeRegexTableIdx++;
            sqlNode.accept(sqlRacketShuttle);

            // Add line spacing for next statement.
            racketInput.add("\n\n");

            isFirstTable = false;
        }

        // add code to run Rosette counterexample engine
        racketInput.add(helpFormatRunCond(likeRegexMatches, orderByMatches, numTablesDefined));

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
     * Formats Rosette like condition for run call for given racket file.
     * @param likeRegexMatches
     * @return String, racket formatted from clause.
     */
    private static String helpFormatRunCond(boolean likeRegexMatches, boolean orderByMatches, int numTables) {
        String likeBool = likeRegexMatches ? "#t" : "#f";
        String orderByBool = orderByMatches ? "#t" : "#f";
        String runCond = "\n(cond\n\t[(eq? #f " + likeBool +
                ") println(\"LIKE regex does not match\")]\n" +
                "\t[(eq? #f " + orderByBool + ") println(\"ORDER BY does not match\")]\n" +
                "\t[(eq? #t " + likeBool + ")" +
                helpFormatRun(numTables) + "])";
        return runCond;
    }

    /**
     * Formats Rosette run call for given racket file.
     * @param numTables
     * @return String, racket formatted from clause.
     */
    private static String helpFormatRun(int numTables) {
        String runCall = "\n\t\t(let* ([model (verify (same q1s q2s))]\n";

        for (int i = 0; i < numTables; i++) {
            runCall = runCall + "\t\t\t   [concrete-t" + (i + 1) + " (clean-ret-table (evaluate " + tableNames.get(i) + " model))]";
            if (i != numTables - 1) {
                runCall = runCall + "\n";
            }
        }
        runCall = runCall + ")\n";

        for (int i = 0; i < numTables; i++) {
            runCall = runCall + "\t\t\t(println concrete-t" + (i + 1) + ")\n";
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
                    toReturnArr.add(" \"" + word2.trim() + "\"");
                }
            }
        }

        toReturnArr.add("\n");

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
            case LIKE:
                List<SqlNode> likeOperands = ((SqlBasicCall) where).getOperandList();
                ArrayList<List<SqlNode>> tableLikeOperands = likeRegex.get(likeRegexTableIdx);

                if (symbolicVals.isEmpty()) {
                    tableLikeOperands.add(likeOperands);
                    likeRegex.set(likeRegexTableIdx, tableLikeOperands);
                } else if (likeRegexToSymbolicVal.containsKey(likeOperands.toString())) {
                    racketInput.add(" (filter-sym (gen-" +  likeRegexToSymbolicVal.get(likeOperands.toString()) + "))");
                } else {
                    racketInput.add(" (filter-sym (gen-" +  symbolicVals.get(likeRegexIdx) + "))");
                    likeRegexToSymbolicVal.put(likeOperands.toString(), symbolicVals.get(likeRegexIdx));
                    likeRegexIdx++;
                }
                break;
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

    private void helpFormatAggr(SqlCall aggr, String aggrSymbol, boolean toBuffer, boolean inHaving) {
        List<SqlNode> operandList;
        operandList = aggr.getOperandList();
        if (toBuffer) {
            ArrayList<String> buffer;
            buffer = aggrStack.peek();
            buffer.add(aggrSymbol);
//            System.out.println(aggr.getOperator());
            for (SqlNode node : operandList) {
                buffer.add("\"" + node.toString() + "\"");
                node.accept(this);
            }
        } else {
            if (inHaving) {
                racketInput.add("VAL-UNOP ");
            }
            racketInput.add(aggrSymbol);
            for (SqlNode node : operandList) {
                node.accept(this);
            }
        }

    }

    private void helpFormatBinopHaving(SqlNode having, String operator) {
        racketInput.add("(BINOP (");
        List<SqlNode> havingOperands = ((SqlCall) having).getOperandList();

        helpFormatHaving(havingOperands.get(0));
        racketInput.add(")");
        racketInput.add(operator);
        helpFormatHaving(havingOperands.get(1));
        racketInput.add(")");
    }

    private void helpFormatHaving(SqlNode having) {

        SqlKind havingType = having.getKind();
        System.out.println(havingType);
        switch (havingType) {
            case LESS_THAN: {
                helpFormatBinopHaving(having, " < ");
                break;
            }
            case GREATER_THAN: {
                System.out.println(having);
                helpFormatBinopHaving(having, " > ");
                break;
            }
            case LESS_THAN_OR_EQUAL: {
                helpFormatBinopHaving(having, " <= ");
                break;
            }
            case GREATER_THAN_OR_EQUAL: {
                helpFormatBinopHaving(having, " >= ");
                break;
            }
            case EQUALS: {
                helpFormatBinopHaving(having, " = ");
                break;
            }
            case OR:
            case AND: {
                List<SqlNode> havingOperands = ((SqlBasicCall) having).getOperandList();
                racketInput.add("(" + havingType + " ");
                helpFormatHaving(havingOperands.get(0));
                helpFormatHaving(havingOperands.get(1));
                racketInput.add(")");
                break;
            }
            case NOT_EQUALS: {
                String[] havingTokens = having.toString().split(" ");
                havingTokens[0] = helpFormatWhereClause(havingTokens[0]);
                havingTokens[1] = "=";
                havingTokens[2] = helpFormatWhereClause(havingTokens[2]);
                racketInput.add(" (NOT (BINOP " + String.join(" ", havingTokens) + ") )");
                break;
            }
            case SUM: {
                helpFormatAggr((SqlCall) having, "aggr-sum", false, true);
                break;
            }
            case COUNT: {
                helpFormatAggr((SqlCall) having, "aggr-count", false, true);
                break;
            }
            case MAX: {
                helpFormatAggr((SqlCall) having, "aggr-max", false, true);
                break;
            }
            case MIN: {
                helpFormatAggr((SqlCall) having, "aggr-min", false, true);
                break;
            }
            case LITERAL: {
                racketInput.add(having.toString());
            }
        }
    }




    // REQUIRED TO IMPLEMENT INTERFACE

    /**
     * Visits a call to a SqlOperator.
     * @param call
     * @return Null, a placeholder required by interface.
     */
    public SqlNode visit(SqlCall call) {
//        System.out.println("SQL CALL");

        boolean withAggr = false;

        SqlKind sqlKind = call.getKind();

        if (call.isA(SqlKind.QUERY)) {
            racketInput.add("(");
            if (isFirstTable) {
                racketInput.add("define q1s (");
            } else {
                racketInput.add("define q2s (");
            }
        }


        switch (sqlKind) {
            case SELECT:
//                System.out.println("\tSQL SELECT\n");
//                racketInput.add("SELECT");

                // if there's a group by, string should have SELECT-GROUP
                // otherwise just SELECT
                SqlSelect sqlSelect = (SqlSelect) call;

                racketInput.add("SELECT");

                List<SqlNode> selectList = sqlSelect.getSelectList();
                if (!selectList.isEmpty()) {
                    racketInput.add(" (VALS");
                }
                for (SqlNode select : selectList) {
                    // if agg func exists in this select query, then push one buffer string to the stack
                    if (!withAggr && select.isA(SqlKind.AGGREGATE)) {
                        withAggr = true;
                        aggrStack.push(new ArrayList<String>());
                    }
                    select.accept(this);
                }
                racketInput.add(")");

                SqlNode from = sqlSelect.getFrom();
                if (from != null) {
                    racketInput.add(" FROM");

                    racketInput.add((" (NAMED"));
                    racketInput.add(helpFormatFrom(from.toString()));
                    racketInput.add(")");
                }

                SqlNode where = sqlSelect.getWhere();
                if (where == null) {
                    racketInput.add(" WHERE (TRUE)");
                } else {
                    racketInput.add(" WHERE");
                    helpFormatWhere(where);
                }

                // jarrett: add SELECT-GROUP
                SqlNodeList sqlGroup = sqlSelect.getGroup();
                //Add group-by statement in racket output
                if (sqlGroup != null) {
                    racketInput.add(" GROUP-BY (list");
                    List<SqlNode> group = sqlGroup.getList();
                    for (SqlNode col: group) {
                        col.accept(this);
                    }
                    racketInput.add(")");
                }

                SqlCall sqlHaving = (SqlCall) sqlSelect.getHaving();
                if (sqlHaving != null) {
//                    System.out.println(sqlHaving.getKind());
                    racketInput.add(" HAVING ");
                    helpFormatHaving(sqlHaving);
//                    sqlHaving.accept(this);
                }

                if (sqlSelect.hasOrderBy()) {
                    if (numTablesDefined == 1) {
                        for (SqlNode s : sqlSelect.getOrderList()) {
                            table1OrderBy.add(s);
                        }
                    } else {
                        ArrayList<SqlNode> currTableOrderBy = new ArrayList<>();
                        for (SqlNode s : sqlSelect.getOrderList()) {
                            currTableOrderBy.add(s);
                        }
                        if (!currTableOrderBy.equals(table1OrderBy)) {
                            orderByMatches = false;
                        }
                    }
                }
                break;
            case JOIN:
                System.out.println("\tSQL JOIN");
                break;

            case SUM:
                helpFormatAggr(call, " aggr-sum ", false, false);
                break;

            case COUNT:
                helpFormatAggr(call, " aggr-count ", false, false);
                break;

            case MAX:
                helpFormatAggr(call, " aggr-max ", false, false);
                break;

            case MIN:
                helpFormatAggr(call, " aggr-min ", false, false);
                break;

        }

        if (withAggr) {
            racketInput.addAll(aggrStack.pop());
        }

        if (!call.isA(SqlKind.AGGREGATE)) {
            racketInput.add("))");
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
//        System.out.println("ID\n");

        racketInput.add(" \"" + id.toString() + "\"");

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
