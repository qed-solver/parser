package org.qed.Generated;

import kala.collection.Seq;
import kala.tuple.Tuple;
import kala.tuple.Tuple2;
import org.qed.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MySQLTester {

    public static String genPath = "src/main/java/org/qed/Generated/MySQL";

    public static void main(String[] args) {
        var rule = new org.qed.Generated.RRuleInstances.FilterMerge();
        new MySQLTester().serialize(rule, genPath);
    }

    public void serialize(RRule rule, String path) {
        var generator = new MySQLGenerator();
        var codeGen = generator.translate(rule.name(), rule.before(), rule.after());
        try {
            Files.createDirectories(Path.of(path));
            Files.write(Path.of(path, rule.name() + ".sql"), codeGen.getBytes());
        } catch (IOException ioe) {
            System.err.println(ioe.getMessage());
        }
    }

}
