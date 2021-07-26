package org.cosette;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.FilenameUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * The Main logic of Cosette-Parser.
 */
public class Main {

    public static void main(String[] args) {
        args = new String[2];
        args[0] = "../examples/example.sql";
        args[1] = "../examples/conditional";
        for (String filename : args) {
            parseFile(filename);
        }
    }

    /**
     * Parse a file or a directory of files.
     *
     * @param path The input path.
     */

    public static void parseFile(String path) {
        String type = FilenameUtils.getExtension(path);
        if (type.equals("sql")) {
            parseSQLFile(path);
        } else if (type.equals("cos")) {
            parseCOSFile(path);
        } else {
            File object = new File(path);
            if (object.isDirectory()) {
                for (File file : Objects.requireNonNull(object.listFiles())) {
                    parseFile(file.getPath());
                }
            }
        }
    }

    /**
     * Parse a .sql file or a .tmp file translated from a .cos file.
     *
     * @param filename The input filename.
     */

    private static void parseSQLFile(String filename) {
        try {
            Scanner scanner = new Scanner(new File(filename));
            SQLParse sqlParse = new SQLParse();
            scanner.useDelimiter(Pattern.compile("\n{2}"));
            while (scanner.hasNext()) {
                String statement = scanner.next();
                try {
                    sqlParse.parseDML(statement);
                } catch (SqlParseException e) {
                    sqlParse.applyDDL(statement);
                }
            }
            String outputPath = FilenameUtils.getPath(filename) + FilenameUtils.getBaseName(filename) + ".json";
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputPath));
            bufferedWriter.write(sqlParse.dumpToJSON());
            bufferedWriter.close();
            sqlParse.done();
            scanner.close();
        } catch (Exception e) {
            System.err.println("In file: " + filename);
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Assuming that the .cos file is always in the following format:<br>
     * schema table_name(column:int, ...);<br>
     * table table_name(table);<br>
     * query _ `query_body`; <br>
     * Then the .cos file will be translated to a .tmp file, which contains the translated SQL statement.
     * The .tmp file will be passed to parseSQLFile(...) and will be deleted after it is used.
     *
     * @param filename The input .cos filename
     */
    private static void parseCOSFile(String filename) {
        try {
            Scanner scanner = new Scanner(new File(filename));
            Pattern schemaPattern = Pattern.compile("(?<=schema\\s)(\\w+)\\((.*)\\)$");
            Pattern declarationPattern = Pattern.compile("(\\w+):\\w+,?\\s?");
            Pattern queryPattern = Pattern.compile("(?<=`)[\\s\\S]*(?=`)");
            StringBuilder sqlBuilder = new StringBuilder();
            scanner.useDelimiter(Pattern.compile(";"));
            while (scanner.hasNext()) {
                String line = scanner.next();
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
            scanner.close();
            String intermediate = FilenameUtils.getPath(filename) + FilenameUtils.getBaseName(filename) + ".tmp";
            File tmp = new File(intermediate);
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tmp));
            bufferedWriter.write(sqlBuilder.toString());
            bufferedWriter.close();
            parseSQLFile(intermediate);
            if (!tmp.delete()) {
                throw new IOException("Failure to delete temporary file: " + intermediate);
            }
        } catch (Exception e) {
            System.err.println("In file: " + filename);
            e.printStackTrace();
            System.exit(-1);
        }
    }

}