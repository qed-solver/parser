package org.qed.Backends.MySQL.Tests;

import org.qed.*;
import org.qed.Backends.MySQL.MySQLGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class MySQLTester {

    public static String genPath = "src/main/java/org/qed/Backends/MySQL/Generated";

    public static String tableName = "testdb.users";
    public static List<String> columnNames = List.of("id", "status");

    public static void main(String[] args) {
        var filterRule = new org.qed.RRuleInstances.FilterMerge();
        new MySQLTester().serializeWithNumericSuffix(filterRule, genPath);

        var projectRule = new org.qed.RRuleInstances.ProjectMerge();
        new MySQLTester().serializeWithNumericSuffix(projectRule, genPath);

        var joinCommute = new org.qed.RRuleInstances.JoinCommute();
        new MySQLTester().serializeWithNumericSuffix(joinCommute, genPath);
    }

    public void serializeWithNumericSuffix(RRule rule, String path) {
        serialize(rule, path, tableName, columnNames, 1);
        serialize(rule, path, tableName, List.of(columnNames.get(1), columnNames.get(0)), 2);
    }

    private void serialize(RRule rule, String path, String tableName, List<String> colNames, int fileIndex) {
        var generator = new MySQLGenerator(tableName, colNames);
        var codeGen = generator.translate(rule.name(), rule.before(), rule.after());
        try {
            Files.createDirectories(Path.of(path));
            String fileName = rule.name() + fileIndex + ".sql";
            Files.write(Path.of(path, fileName), codeGen.getBytes());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }
}
