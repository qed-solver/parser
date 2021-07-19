package org.cosette;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.ValidationException;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    public static void main(String[] args) {
        args = new String[2];
        args[0] = "../examples/example.sql";
        args[1] = "../examples/conditional/fkPennTR.cos";
        for (String filename : args) {
            parseFile(filename);
        }
    }

    public static void parseFile(String filename) {
        try {
            String type = FilenameUtils.getExtension(filename);
            Scanner scanner = new Scanner(new File(filename));
            if (type.equals("sql")) {
                parseSQLFile(scanner);
            } else if (type.equals("cos")) {
                parseCOSFile(scanner);
            } else {
                System.err.println("Not supported file type: " + type);
            }
        } catch (Exception e) {
            System.err.println("In file: " + filename);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private static void parseSQLFile(Scanner content) throws ValidationException, JsonProcessingException, SQLException {
        SQLParse sqlParse = new SQLParse();
        content.useDelimiter(Pattern.compile("\n{2}"));
        while (content.hasNext()) {
            String statement = content.next();
            try {
                sqlParse.parseDML(statement);
            } catch (SqlParseException e) {
                sqlParse.applyDDL(statement);
            }
        }
        System.out.println(sqlParse.dumpToJSON());
        sqlParse.done();
    }

    private static void parseCOSFile(Scanner content) throws ValidationException, SQLException, JsonProcessingException {
        Pattern schemaPattern = Pattern.compile("(?<=schema\\s)(\\w+)\\((.*)\\)$");
        Pattern declarationPattern = Pattern.compile("(\\w+):\\w+,?\\s?");
        Pattern queryPattern = Pattern.compile("(?<=`)[\\s\\S]*(?=`)");
        StringBuilder sqlBuilder = new StringBuilder();
        content.useDelimiter(Pattern.compile(";"));
        while (content.hasNext()) {
            String line = content.next();
            Matcher schemaMatcher = schemaPattern.matcher(line);
            Matcher queryMatcher = queryPattern.matcher(line);
            if (schemaMatcher.find()) {
                Matcher declarationMatcher = declarationPattern.matcher(schemaMatcher.group(2));
                sqlBuilder.append("CREATE TABLE ");
                sqlBuilder.append(schemaMatcher.group(1).toUpperCase(Locale.ROOT));
                sqlBuilder.append(" (");
                while (declarationMatcher.find()) {
                    sqlBuilder.append("\n\t");
                    sqlBuilder.append(declarationMatcher.group(1).toUpperCase(Locale.ROOT));
                    sqlBuilder.append(" INTEGER,");
                }
                if (declarationMatcher.reset().find()) {
                    sqlBuilder.deleteCharAt(sqlBuilder.length() - 1);
                    sqlBuilder.append("\n");
                }
                sqlBuilder.append(")\n\n");
            }
            if (queryMatcher.find()) {
                sqlBuilder.append(queryMatcher.group().toUpperCase(Locale.ROOT));
                sqlBuilder.append("\n\n");
            }
        }
        parseSQLFile(new Scanner(sqlBuilder.toString()));
    }

}